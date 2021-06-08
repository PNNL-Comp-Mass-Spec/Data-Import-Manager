using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class clsProcessXmlTriggerFile : clsLoggerBase
    {
        // Ignore Spelling: prepend, MM-dd-yyyy, logon, Bionet

        #region "Constants and Structures"

        private const string CHECK_THE_LOG_FOR_DETAILS = "Check the log for details";

        public struct XmlProcSettingsType
        {
            public int DebugLevel;
            public bool IgnoreInstrumentSourceErrors;
            public bool PreviewMode;
            public bool TraceMode;
            public string FailureDirectory;
            public string SuccessDirectory;
        }

        #endregion

        #region "Properties"

        public XmlProcSettingsType ProcSettings { get; set; }

        /// <summary>
        /// Mail message(s) that need to be sent
        /// </summary>
        public ConcurrentDictionary<string, ConcurrentBag<clsQueuedMail>> QueuedMail { get; }

        #endregion

        #region "Member Variables"

        private readonly MgrSettings mMgrSettings;

        private readonly ConcurrentDictionary<string, int> mInstrumentsToSkip;

        // ReSharper disable once InconsistentNaming
        private readonly DMSInfoCache mDMSInfoCache;

        private clsDataImportTask mDataImportTask;

        private string mDatabaseErrorMsg;

        private bool mSecondaryLogonServiceChecked;

        private string mXmlOperatorName = string.Empty;

        private string mXmlOperatorEmail = string.Empty;

        /// <summary>
        /// Path to the dataset on the instrument
        /// </summary>
        private string mXmlDatasetPath = string.Empty;

        private string mXmlInstrumentName = string.Empty;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrSettings"></param>
        /// <param name="instrumentsToSkip"></param>
        /// <param name="infoCache"></param>
        /// <param name="udtSettings"></param>
        public clsProcessXmlTriggerFile(
            MgrSettings mgrSettings,
            ConcurrentDictionary<string, int> instrumentsToSkip,
            DMSInfoCache infoCache,
            XmlProcSettingsType udtSettings)
        {
            mMgrSettings = mgrSettings;
            mInstrumentsToSkip = instrumentsToSkip;
            ProcSettings = udtSettings;

            mDMSInfoCache = infoCache;

            QueuedMail = new ConcurrentDictionary<string, ConcurrentBag<clsQueuedMail>>();
        }

        private void CacheMail(List<clsValidationError> validationErrors, string addnlRecipient, string subjectAppend)
        {
            var enableEmail = mMgrSettings.GetParam("EnableEmail", false);
            if (!enableEmail)
            {
                return;
            }

            try
            {
                var mailRecipients = mMgrSettings.GetParam("to");
                var mailRecipientsList = mailRecipients.Split(';').Distinct().ToList();

                // Possibly update the e-mail address for addnlRecipient
                if (!string.IsNullOrEmpty(addnlRecipient) && !mailRecipientsList.Contains(addnlRecipient))
                {
                    mailRecipients += ";" + addnlRecipient;
                }

                // Define the Subject
                string mailSubject;
                if (string.IsNullOrEmpty(subjectAppend))
                {
                    // Data Import Manager
                    mailSubject = mMgrSettings.GetParam("subject");
                }
                else
                {
                    // Data Import Manager - Appended Info
                    mailSubject = mMgrSettings.GetParam("subject") + subjectAppend;
                }

                // Store the message and metadata
                var messageToQueue = new clsQueuedMail(mXmlOperatorName, mailRecipients, mailSubject, validationErrors);
                if (!string.IsNullOrEmpty(mDatabaseErrorMsg))
                {
                    messageToQueue.DatabaseErrorMsg = mDatabaseErrorMsg;
                }

                messageToQueue.InstrumentDatasetPath = mXmlDatasetPath;

                // Queue the message
                if (QueuedMail.TryGetValue(mailRecipients, out var existingQueuedMessages))
                {
                    existingQueuedMessages.Add(messageToQueue);
                }
                else
                {
                    var newQueuedMessages = new ConcurrentBag<clsQueuedMail>
                    {
                        messageToQueue
                    };

                    if (QueuedMail.TryAdd(mailRecipients, newQueuedMessages))
                        return;

                    if (QueuedMail.TryGetValue(mailRecipients, out existingQueuedMessages))
                    {
                        existingQueuedMessages.Add(messageToQueue);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error sending email message", ex);
            }
        }

        /// <summary>
        /// Returns a string with the path to the log file, assuming the file can be accessed with \\ComputerName\DMS_Programs\Manager\Logs\LogFileName.txt
        /// </summary>
        private string GetLogFileSharePath()
        {
            var logFileName = mMgrSettings.GetParam("LogFileName");
            return GetLogFileSharePath(logFileName);
        }

        /// <summary>
        /// Returns a string with the path to the log file, assuming the file can be accessed with \\ComputerName\DMS_Programs\Manager\Logs\LogFileName.txt
        /// </summary>
        /// <param name="baseLogFileName">Base name of the current log file, e.g. Logs\DataImportManager</param>
        public static string GetLogFileSharePath(string baseLogFileName)
        {
            var exeDirectoryPath = clsGlobal.GetExeDirectoryPath();

            var exeDirectoryName = Path.GetFileName(exeDirectoryPath);

            var exeDirectoryNameToUse = string.IsNullOrWhiteSpace(exeDirectoryName) ? "DataImportManager" : exeDirectoryName;

            var logFilePath = Path.Combine(exeDirectoryNameToUse, string.IsNullOrEmpty(baseLogFileName) ? @"Logs\DataImportManager" : baseLogFileName);

            // logFilePath should look like this:
            //    DataImportManager\Logs\DataImportManager

            // Prepend the computer name and share name then append the current date, giving a share path like:
            // \\Proto-6\DMS_Programs\DataImportManager\Logs\DataImportManager_04-09-2018.txt
            var logSharePath = @"\\" + clsGlobal.GetHostName() + @"\DMS_Programs\" + logFilePath +
                              "_" + DateTime.Now.ToString("MM-dd-yyyy") + ".txt";

            return logSharePath;
        }

        /// <summary>
        /// Validate the XML trigger file, then send it to the database using mDataImportTask.PostTask
        /// </summary>
        /// <param name="triggerFile"></param>
        /// <returns>True if success, false if an error</returns>
        public bool ProcessFile(FileInfo triggerFile)
        {
            mDatabaseErrorMsg = string.Empty;

            var statusMsg = "Starting data import task for dataset: " + triggerFile.FullName;
            if (ProcSettings.TraceMode)
            {
                Console.WriteLine();
                Console.WriteLine("-------------------------------------------");
            }

            LogMessage(statusMsg);

            var triggerFileInfo = new TriggerFileInfo(triggerFile);
            if (!ValidateXmlFileMain(triggerFileInfo))
            {
                if (mSecondaryLogonServiceChecked)
                {
                    return false;
                }

                mSecondaryLogonServiceChecked = true;

                // Check the status of the Secondary Logon service

                // ReSharper disable once StringLiteralTypo
                var sc = new ServiceController("seclogon");
                if (sc.Status == ServiceControllerStatus.Running)
                {
                    return false;
                }

                clsMainProcess.LogErrorToDatabase("The Secondary Logon service is not running; this is required to access files on Bionet");
                try
                {
                    // Try to start it
                    LogMessage("Attempting to start the Secondary Logon service");

                    sc.Start();

                    ConsoleMsgUtils.SleepSeconds(3);

                    statusMsg = "Successfully started the Secondary Logon service (normally should be running, but found to be stopped)";
                    LogWarning(statusMsg, true);

                    // Now that the service is running, try the validation one more time
                    if (!ValidateXmlFileMain(triggerFileInfo))
                    {
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    LogWarning("Unable to start the Secondary Logon service: " + ex.Message);
                    return false;
                }
            }

            if (!triggerFile.Exists)
            {
                LogWarning("XML file no longer exists; cannot import: " + triggerFile.FullName);
                return false;
            }

            if (ProcSettings.DebugLevel >= 2)
            {
                LogMessage("Posting Dataset XML file to database: " + triggerFile.Name);
            }

            // Open a new database connection
            // Doing this now due to database timeouts that were seen when using mDMSInfoCache.DBConnection
            var dbTools = mDMSInfoCache.DBTools;

            // Create the object that will import the Data record
            //
            mDataImportTask = new clsDataImportTask(mMgrSettings, dbTools)
            {
                TraceMode = ProcSettings.TraceMode,
                PreviewMode = ProcSettings.PreviewMode
            };

            mDatabaseErrorMsg = string.Empty;
            var success = mDataImportTask.PostTask(triggerFileInfo);

            mDatabaseErrorMsg = mDataImportTask.DatabaseErrorMessage;

            if (mDatabaseErrorMsg.IndexOf("timeout expired.", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Log the error and leave the file for another attempt
                clsMainProcess.LogErrorToDatabase("Encountered database timeout error for dataset: " + triggerFile.FullName);
                return false;
            }

            if (success)
            {
                MoveXmlFile(triggerFile, ProcSettings.SuccessDirectory);
                LogMessage("Completed Data import task for dataset: " + triggerFile.FullName);
                return true;
            }

            // Look for:
            // Transaction (Process ID 67) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction
            if (mDataImportTask.PostTaskErrorMessage.IndexOf("deadlocked", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Log the error and leave the file for another attempt
                statusMsg = "Deadlock encountered";
                LogError(statusMsg + ": " + triggerFile.Name);
                return false;
            }

            // Look for:
            // The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction
            if (mDataImportTask.PostTaskErrorMessage.IndexOf("current transaction cannot be committed", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Log the error and leave the file for another attempt
                statusMsg = "Transaction commit error";
                LogError(statusMsg + ": " + triggerFile.Name);
                return false;
            }

            BaseLogger.LogLevels messageType;

            var moveLocPath = MoveXmlFile(triggerFile, ProcSettings.FailureDirectory);
            statusMsg = "Error posting xml file to database: " + mDataImportTask.PostTaskErrorMessage;

            if (mDataImportTask.PostTaskErrorMessage.IndexOf("since already in database", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                messageType = BaseLogger.LogLevels.WARN;
                LogWarning(statusMsg + ". See: " + moveLocPath);
            }
            else
            {
                messageType = BaseLogger.LogLevels.ERROR;
                clsMainProcess.LogErrorToDatabase(statusMsg + ". View details in log at " + GetLogFileSharePath() + " for: " + moveLocPath);
            }

            var validationErrors = new List<clsValidationError>();
            var newError = new clsValidationError("XML trigger file problem", moveLocPath);

            string msgTypeString;
            if (messageType == BaseLogger.LogLevels.ERROR)
            {
                msgTypeString = "Error";
            }
            else
            {
                msgTypeString = "Warning";
            }

            if (string.IsNullOrWhiteSpace(mDataImportTask.PostTaskErrorMessage))
            {
                newError.AdditionalInfo = msgTypeString + ": " + CHECK_THE_LOG_FOR_DETAILS;
            }
            else
            {
                newError.AdditionalInfo = msgTypeString + ": " + mDataImportTask.PostTaskErrorMessage;
            }

            validationErrors.Add(newError);

            // Check whether there is a suggested solution in table T_DIM_Error_Solution for the error
            var errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg);
            if (!string.IsNullOrWhiteSpace(errorSolution))
            {
                // Store the solution in the database error message variable so that it gets included in the message body
                mDatabaseErrorMsg = errorSolution;
            }

            // Send an e-mail; subject will be "Data Import Manager - Database error." or "Data Import Manager - Database warning."
            CacheMail(validationErrors, mXmlOperatorEmail, " - Database " + msgTypeString.ToLower() + ".");
            return false;
        }

        /// <summary>
        /// Move a trigger file to the target directory
        /// </summary>
        /// <param name="triggerFile"></param>
        /// <param name="targetDirectory"></param>
        /// <returns>New path of the trigger file</returns>
        private string MoveXmlFile(FileInfo triggerFile, string targetDirectory)
        {
            try
            {
                if (!triggerFile.Exists)
                {
                    return string.Empty;
                }

                if (!Directory.Exists(targetDirectory))
                {
                    if (ProcSettings.TraceMode)
                    {
                        ShowTraceMessage("Creating target directory: " + targetDirectory);
                    }

                    Directory.CreateDirectory(targetDirectory);
                }

                var targetFilePath = Path.Combine(targetDirectory, triggerFile.Name);
                if (ProcSettings.TraceMode)
                {
                    ShowTraceMessage("Instantiating file info var for " + targetFilePath);
                }

                var xmlFileNewLoc = new FileInfo(targetFilePath);
                if (xmlFileNewLoc.Exists)
                {
                    if (ProcSettings.PreviewMode)
                    {
                        ShowTraceMessage("Preview: delete target file: " + xmlFileNewLoc.FullName);
                    }
                    else
                    {
                        if (ProcSettings.TraceMode)
                        {
                            ShowTraceMessage("Deleting target file: " + xmlFileNewLoc.FullName);
                        }

                        xmlFileNewLoc.Delete();
                    }
                }

                var movePaths =
                    "XML file " + Environment.NewLine +
                    "  from " + triggerFile.FullName + Environment.NewLine +
                    "  to   " + xmlFileNewLoc.DirectoryName;

                if (ProcSettings.PreviewMode)
                {
                    ShowTraceMessage("Preview: move " + movePaths);
                }
                else
                {
                    if (ProcSettings.TraceMode)
                    {
                        ShowTraceMessage("Moving " + movePaths);
                    }

                    triggerFile.MoveTo(xmlFileNewLoc.FullName);
                }

                return xmlFileNewLoc.FullName;
            }
            catch (Exception ex)
            {
                LogError("Exception in MoveXmlFile", ex);
                return string.Empty;
            }
        }

        /// <summary>
        /// Adds or updates instrumentName in m_InstrumentsToSkip
        /// </summary>
        /// <param name="instrumentName"></param>
        private void UpdateInstrumentsToSkip(string instrumentName)
        {
            // Look for the instrument in m_InstrumentsToSkip
            if (mInstrumentsToSkip.TryGetValue(instrumentName, out var datasetsSkipped))
            {
                mInstrumentsToSkip[instrumentName] = datasetsSkipped + 1;
                return;
            }

            // Instrument not found; add it
            if (mInstrumentsToSkip.TryAdd(instrumentName, 1))
                return;

            // Instrument add failed; try again to get the datasets skipped value
            if (mInstrumentsToSkip.TryGetValue(instrumentName, out var datasetsSkippedRetry))
            {
                mInstrumentsToSkip[instrumentName] = datasetsSkippedRetry + 1;
            }
        }

        /// <summary>
        /// Process the specified XML file
        /// </summary>
        /// <param name="triggerFileInfo">XML file to process</param>
        /// <returns>True if XML file is valid and dataset is ready for import; otherwise false</returns>
        /// <remarks>
        /// PerformValidation in clsXMLTimeValidation will monitor the dataset file (or dataset directory)
        /// to make sure the file size (directory size) remains unchanged over 30 seconds (see VerifyConstantFileSize and VerifyConstantDirectorySize)
        /// </remarks>
        private bool ValidateXmlFileMain(TriggerFileInfo triggerFileInfo)
        {
            try
            {
                var timeValidationDirectory = mMgrSettings.GetParam("TimeValidationFolder");
                string moveLocPath;
                var failureDirectory = mMgrSettings.GetParam("FailureFolder");

                var myDataXmlValidation = new clsXMLTimeValidation(mMgrSettings, mInstrumentsToSkip, mDMSInfoCache, ProcSettings)
                {
                    TraceMode = ProcSettings.TraceMode
                };

                var xmlResult = myDataXmlValidation.ValidateXmlFile(triggerFileInfo);

                mXmlOperatorName = myDataXmlValidation.OperatorName;
                mXmlOperatorEmail = myDataXmlValidation.OperatorEMail;
                mXmlDatasetPath = myDataXmlValidation.DatasetPath;
                mXmlInstrumentName = myDataXmlValidation.InstrumentName;

                var triggerFile = triggerFileInfo.TriggerFile;
                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_NO_OPERATOR)
                {
                    moveLocPath = MoveXmlFile(triggerFile, failureDirectory);

                    LogWarning("Undefined Operator in " + moveLocPath, true);

                    var validationErrors = new List<clsValidationError>();

                    if (string.IsNullOrWhiteSpace(mXmlOperatorName))
                    {
                        validationErrors.Add(new clsValidationError("Operator name not listed in the XML file", string.Empty));
                    }
                    else
                    {
                        validationErrors.Add(new clsValidationError("Operator name not defined in DMS", mXmlOperatorName));
                    }

                    validationErrors.Add(new clsValidationError("Dataset trigger file path", moveLocPath));

                    mDatabaseErrorMsg = "Operator payroll number/HID was blank";
                    var errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg);

                    if (string.IsNullOrWhiteSpace(errorSolution))
                    {
                        mDatabaseErrorMsg = string.Empty;
                    }
                    else
                    {
                        mDatabaseErrorMsg = errorSolution;
                    }

                    CacheMail(validationErrors, mXmlOperatorEmail, " - Operator not defined.");
                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_FAILED)
                {
                    moveLocPath = MoveXmlFile(triggerFile, timeValidationDirectory);

                    LogWarning("XML Time validation error, file " + moveLocPath);
                    clsMainProcess.LogErrorToDatabase("Time validation error. View details in log at " + GetLogFileSharePath() + " for: " + moveLocPath);

                    var validationErrors = new List<clsValidationError>
                    {
                        new("Time validation error", moveLocPath)
                    };

                    CacheMail(validationErrors, mXmlOperatorEmail, " - Time validation error.");
                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR)
                {
                    moveLocPath = MoveXmlFile(triggerFile, failureDirectory);

                    LogWarning("An error was encountered during the validation process, file " + moveLocPath, true);

                    var validationErrors = new List<clsValidationError>
                    {
                        new("XML error encountered during validation process", moveLocPath)
                    };

                    CacheMail(validationErrors, mXmlOperatorEmail, " - XML validation error.");
                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE)
                {
                    // Logon failure; Do not move the XML file
                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR)
                {
                    // Network error; Do not move the XML file
                    // Furthermore, do not process any more .XML files for this instrument
                    UpdateInstrumentsToSkip(mXmlInstrumentName);
                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_SKIP_INSTRUMENT)
                {
                    LogMessage(" ... skipped since m_InstrumentsToSkip contains " + mXmlInstrumentName);
                    UpdateInstrumentsToSkip(mXmlInstrumentName);
                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_WAIT_FOR_FILES)
                {
                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED)
                {
                    // Size changed; Do not move the XML file
                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_NO_DATA)
                {
                    moveLocPath = MoveXmlFile(triggerFile, failureDirectory);

                    LogWarning("Dataset " + myDataXmlValidation.DatasetName + " not found at " + myDataXmlValidation.SourcePath, true);

                    var validationErrors = new List<clsValidationError>();

                    var newError = new clsValidationError("Dataset not found on the instrument", moveLocPath);
                    if (string.IsNullOrEmpty(myDataXmlValidation.ErrorMessage))
                    {
                        newError.AdditionalInfo = string.Empty;
                    }
                    else
                    {
                        newError.AdditionalInfo = myDataXmlValidation.ErrorMessage;
                    }

                    validationErrors.Add(newError);

                    mDatabaseErrorMsg = "The dataset data is not available for capture";
                    var errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg);
                    if (string.IsNullOrWhiteSpace(errorSolution))
                    {
                        mDatabaseErrorMsg = string.Empty;
                    }
                    else
                    {
                        mDatabaseErrorMsg = errorSolution;
                    }

                    CacheMail(validationErrors, mXmlOperatorEmail, " - Dataset not found.");

                    return false;
                }

                if (xmlResult == clsXMLTimeValidation.XmlValidateStatus.XML_VALIDATE_TRIGGER_FILE_MISSING)
                {
                    // The file is now missing; silently move on
                    return false;
                }

                // xmlResult is one of the following:
                // XML_VALIDATE_SUCCESS
                // XML_VALIDATE_CONTINUE
                // XML_VALIDATE_SKIP_INSTRUMENT

                return true;
            }
            catch (Exception ex)
            {
                clsMainProcess.LogErrorToDatabase("Error validating Xml Data file, file " + triggerFileInfo.TriggerFile.FullName, ex);
                return false;
            }
        }

        private void ShowTraceMessage(string message)
        {
            clsMainProcess.ShowTraceMessage(message);
        }
    }
}
