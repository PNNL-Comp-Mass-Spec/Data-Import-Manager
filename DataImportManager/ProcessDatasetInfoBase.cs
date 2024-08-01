using PRISM.AppSettings;
using PRISM.Logging;
using PRISM;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceProcess;

namespace DataImportManager
{
    internal class ProcessDatasetInfoBase : LoggerBase
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: Bionet, logon, MM-dd-yyyy, prepend, seclogon

        private const string CHECK_THE_LOG_FOR_DETAILS = "Check the log for details";

        /// <summary>
        /// Processing settings struct
        /// </summary>
        public struct XmlProcSettingsType
        {
            /// <summary>
            /// Debug level
            /// </summary>
            /// <remarks>Higher values lead to more log messages</remarks>
            public int DebugLevel;

            /// <summary>
            /// When true, ignore instrument source errors
            /// </summary>
            public bool IgnoreInstrumentSourceErrors;

            /// <summary>
            /// When true, preview adding new datasets
            /// </summary>
            public bool PreviewMode;

            /// <summary>
            /// When true, show additional messages
            /// </summary>
            public bool TraceMode;

            /// <summary>
            /// Share path for failed XML trigger files
            /// </summary>
            public string FailureDirectory;

            /// <summary>
            /// Share path for successful XML trigger files
            /// </summary>
            public string SuccessDirectory;
        }

        /// <summary>
        /// Processing settings
        /// </summary>
        public XmlProcSettingsType ProcSettings { get; set; }

        /// <summary>
        /// Mail message(s) that need to be sent
        /// </summary>
        public ConcurrentDictionary<string, ConcurrentBag<QueuedMail>> QueuedMail { get; }

        private readonly MgrSettings mMgrSettings;

        private readonly ConcurrentDictionary<string, int> mInstrumentsToSkip;

        // ReSharper disable once InconsistentNaming
        private readonly DMSInfoCache mDMSInfoCache;

        private DataImportTask mDataImportTask;

        /// <summary>
        /// Error message to include in the e-mail to the user
        /// </summary>
        public string ErrorMessageForUser { get; protected set; }

        /// <summary>
        /// Error message to store in the database (only applies to dataset create tasks)
        /// </summary>
        public string ErrorMessageForDatabase { get; protected set; }

        private bool mSecondaryLogonServiceChecked;

        private string mXmlOperatorName = string.Empty;

        private string mXmlOperatorEmail = string.Empty;

        /// <summary>
        /// Path to the dataset on the instrument
        /// </summary>
        private string mXmlDatasetPath = string.Empty;

        private string mXmlInstrumentName = string.Empty;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrSettings"></param>
        /// <param name="instrumentsToSkip"></param>
        /// <param name="infoCache"></param>
        /// <param name="udtSettings"></param>
        public ProcessDatasetInfoBase(
            MgrSettings mgrSettings,
            ConcurrentDictionary<string, int> instrumentsToSkip,
            DMSInfoCache infoCache,
            XmlProcSettingsType udtSettings)
        {
            mMgrSettings = mgrSettings;
            mInstrumentsToSkip = instrumentsToSkip;
            ProcSettings = udtSettings;

            mDMSInfoCache = infoCache;

            QueuedMail = new ConcurrentDictionary<string, ConcurrentBag<QueuedMail>>();
        }

        private static string AppendToText(string existingText, string additionalText)
        {
            if (string.IsNullOrWhiteSpace(additionalText))
                return existingText;

            return string.IsNullOrWhiteSpace(existingText)
                ? additionalText
                : string.Format("{0}; {1}", existingText, additionalText);
        }

        private void CacheMail(List<ValidationError> validationErrors, string addnlRecipient, string subjectAppend)
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
                var messageToQueue = new QueuedMail(mXmlOperatorName, mailRecipients, mailSubject, validationErrors);

                if (!string.IsNullOrEmpty(ErrorMessageForUser))
                {
                    messageToQueue.ErrorMessageForUser = ErrorMessageForUser;
                }

                messageToQueue.InstrumentDatasetPath = mXmlDatasetPath;

                // Queue the message
                if (QueuedMail.TryGetValue(mailRecipients, out var existingQueuedMessages))
                {
                    existingQueuedMessages.Add(messageToQueue);
                }
                else
                {
                    var newQueuedMessages = new ConcurrentBag<QueuedMail>
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

        protected virtual void FinalizeTask()
        {
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
            var exeDirectoryPath = Global.GetExeDirectoryPath();

            var exeDirectoryName = Path.GetFileName(exeDirectoryPath);

            var exeDirectoryNameToUse = string.IsNullOrWhiteSpace(exeDirectoryName) ? "DataImportManager" : exeDirectoryName;

            var logFilePath = Path.Combine(exeDirectoryNameToUse, string.IsNullOrEmpty(baseLogFileName) ? @"Logs\DataImportManager" : baseLogFileName);

            // logFilePath should look like this:
            //    DataImportManager\Logs\DataImportManager

            // Prepend the computer name and share name then append the current date, giving a share path like:
            // \\Proto-6\DMS_Programs\DataImportManager\Logs\DataImportManager_2022-06-11.txt

            return string.Format(@"\\{0}\DMS_Programs\{1}_{2:yyyy-MM-dd}.txt",
                    Global.GetHostName(), logFilePath, DateTime.Now);
        }

        /// <summary>
        /// Validate the metadata for the new dataset, then send it to the database using mDataImportTask.PostTask
        /// </summary>
        /// <param name="captureInfo">Dataset capture info</param>
        /// <returns>True if success, false if an error</returns>
        protected bool ProcessDatasetCaptureInfo(DatasetCaptureInfo captureInfo)
        {
            if (!ValidateXmlInfoMain(captureInfo))
            {
                if (mSecondaryLogonServiceChecked)
                {
                    UpdateErrorMessagePropertiesIfEmpty("Call to ValidateXmlInfoMain returned false");
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

                MainProcess.LogErrorToDatabase("The Secondary Logon service is not running; this is required to access files on Bionet");

                try
                {
                    // Try to start it
                    LogMessage("Attempting to start the Secondary Logon service");

                    sc.Start();

                    ConsoleMsgUtils.SleepSeconds(3);

                    LogWarning("Successfully started the Secondary Logon service (normally should be running, but found to be stopped)", true);

                    // Now that the service is running, try the validation one more time
                    if (!ValidateXmlInfoMain(captureInfo))
                    {
                        UpdateErrorMessagePropertiesIfEmpty("Call to ValidateXmlInfoMain returned false");
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    var msg = "Unable to start the Secondary Logon service: " + ex.Message;

                    ErrorMessageForUser = AppendToText(ErrorMessageForUser, msg);
                    ErrorMessageForDatabase = AppendToText(ErrorMessageForDatabase, msg);

                    LogWarning(ErrorMessageForUser);
                    return false;
                }
            }

            if (captureInfo is TriggerFileInfo xmlTriggerFileInfo)
            {
                if (!xmlTriggerFileInfo.TriggerFile.Exists)
                {
                    LogWarning("XML file no longer exists; cannot import: " + xmlTriggerFileInfo.TriggerFile.FullName);
                    return false;
                }
            }
            else
            {
                xmlTriggerFileInfo = null;
            }

            if (ProcSettings.DebugLevel >= 2)
            {
                LogMessage("Posting dataset XML to database: " + captureInfo.GetSourceDescription());
            }

            // Open a new database connection
            // Doing this now due to database timeouts that were seen when using mDMSInfoCache.DBConnection
            var dbTools = mDMSInfoCache.DBTools;

            // Create the object that will import the metadata
            mDataImportTask = new DataImportTask(mMgrSettings, dbTools)
            {
                TraceMode = ProcSettings.TraceMode,
                PreviewMode = ProcSettings.PreviewMode
            };

            var success = mDataImportTask.PostTask(captureInfo);

            ErrorMessageForUser = mDataImportTask.DataImportErrorMessage;
            ErrorMessageForDatabase = mDataImportTask.DataImportErrorMessageForDatabase;

            if (ErrorMessageForUser.IndexOf("timeout expired.", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Log the error and leave the file for another attempt
                MainProcess.LogErrorToDatabase("Encountered database timeout error for dataset: " + captureInfo.GetSourceDescription(true));
                return false;
            }

            if (success)
            {
                FinalizeTask();
                return true;
            }

            // Look for:
            // Transaction (Process ID 67) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction
            if (mDataImportTask.PostTaskErrorMessage.IndexOf("deadlocked", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Log the error and leave the file for another attempt
                LogError("Deadlock encountered: " + captureInfo.GetSourceDescription());

                UpdateErrorMessagePropertiesIfEmpty("Deadlock encountered");
                return false;
            }

            // Look for:
            // The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction
            if (mDataImportTask.PostTaskErrorMessage.IndexOf("current transaction cannot be committed", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Log the error and leave the file for another attempt
                LogError("Transaction commit error: " + captureInfo.GetSourceDescription());

                UpdateErrorMessagePropertiesIfEmpty("Transaction commit error");
                return false;
            }

            BaseLogger.LogLevels messageType;

            string sourceDescription;

            if (xmlTriggerFileInfo == null)
            {
                sourceDescription = captureInfo.GetSourceDescription();
            }
            else
            {
                sourceDescription = MoveXmlFile(xmlTriggerFileInfo.TriggerFile, ProcSettings.FailureDirectory);
            }

            var errorMessage = "Error posting dataset XML to database: " + mDataImportTask.PostTaskErrorMessage;

            if (mDataImportTask.PostTaskErrorMessage.IndexOf("since already in database", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                messageType = BaseLogger.LogLevels.WARN;
                LogWarning(errorMessage + ". See: " + sourceDescription);
            }
            else
            {
                messageType = BaseLogger.LogLevels.ERROR;
                MainProcess.LogErrorToDatabase(errorMessage + ". View details in log at " + GetLogFileSharePath() + " for: " + captureInfo.GetSourceDescription(true));
            }

            ErrorMessageForUser = AppendToText(ErrorMessageForUser, errorMessage);
            ErrorMessageForDatabase = AppendToText(ErrorMessageForDatabase, errorMessage);

            var validationErrors = new List<ValidationError>();
            var newError = new ValidationError("Dataset XML problem", sourceDescription);

            var msgTypeString = messageType == BaseLogger.LogLevels.ERROR ? "Error" : "Warning";

            if (string.IsNullOrWhiteSpace(mDataImportTask.PostTaskErrorMessage))
            {
                newError.AdditionalInfo = msgTypeString + ": " + CHECK_THE_LOG_FOR_DETAILS;
            }
            else
            {
                newError.AdditionalInfo = msgTypeString + ": " + mDataImportTask.PostTaskErrorMessage;

                ErrorMessageForDatabase = AppendToText(ErrorMessageForDatabase, mDataImportTask.PostTaskErrorMessage);
            }

            validationErrors.Add(newError);

            // Check whether there is a suggested solution in table T_DIM_Error_Solution for the error
            var errorSolution = mDMSInfoCache.GetDbErrorSolution(ErrorMessageForUser);

            if (!string.IsNullOrWhiteSpace(errorSolution))
            {
                if (xmlTriggerFileInfo == null)
                {
                    LogWarning(errorSolution);
                }
                else
                {
                    // Store the solution in the database error message variable so that it gets included in the message body of the e-mail
                    ErrorMessageForUser = errorSolution;
                }
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
        protected string MoveXmlFile(FileInfo triggerFile, string targetDirectory)
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
        /// Process the specified dataset XML
        /// </summary>
        /// <remarks>
        /// PerformValidation in XMLTimeValidation will monitor the dataset file (or dataset directory)
        /// to make sure the file size (directory size) remains unchanged over 30 seconds (see VerifyConstantFileSize and VerifyConstantDirectorySize)
        /// </remarks>
        /// <param name="captureInfo">Dataset capture info</param>
        /// <returns>True if dataset metadata is valid the and dataset is ready for import; otherwise false</returns>
        private bool ValidateXmlInfoMain(DatasetCaptureInfo captureInfo)
        {
            try
            {
                var timeValidationDirectory = mMgrSettings.GetParam("TimeValidationFolder");

                var failureDirectory = mMgrSettings.GetParam("FailureFolder");

                var myDataXmlValidation = new XMLTimeValidation(mMgrSettings, mInstrumentsToSkip, mDMSInfoCache, ProcSettings)
                {
                    TraceMode = ProcSettings.TraceMode
                };

                XMLTimeValidation.XmlValidateStatus xmlResult;
                FileInfo triggerFile;
                string sourceDescriptionPrefix;

                switch (captureInfo)
                {
                    case TriggerFileInfo xmlTriggerFileInfo:
                        xmlResult = myDataXmlValidation.ValidateXmlFile(xmlTriggerFileInfo);

                        // Processing an actual Xml Trigger File
                        triggerFile = xmlTriggerFileInfo.TriggerFile;
                        sourceDescriptionPrefix = "the XML file";
                        break;

                    case DatasetCreateTaskInfo createTaskInfo:
                        xmlResult = myDataXmlValidation.ValidateDatasetCreateTaskXml(createTaskInfo);

                        // Processing data from a dataset creation task
                        triggerFile = null;
                        sourceDescriptionPrefix = captureInfo.GetSourceDescription();
                        break;

                    default:
                        throw new Exception("Unrecognized data type for the captureInfo argument in ValidateXmlInfoMain");
                }

                mXmlOperatorName = myDataXmlValidation.OperatorName;
                mXmlOperatorEmail = myDataXmlValidation.OperatorEMail;
                mXmlDatasetPath = myDataXmlValidation.DatasetPath;
                mXmlInstrumentName = myDataXmlValidation.InstrumentName;

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_NO_OPERATOR)
                {
                    string moveLocPath;

                    if (triggerFile == null)
                    {
                        moveLocPath = string.Empty;
                        LogWarning("Undefined Operator in " + captureInfo.GetSourceDescription(), true);
                    }
                    else
                    {
                        moveLocPath = MoveXmlFile(triggerFile, failureDirectory);
                        LogWarning("Undefined Operator in " + moveLocPath, true);
                    }

                    var validationErrors = new List<ValidationError>();

                    if (string.IsNullOrWhiteSpace(mXmlOperatorName))
                    {
                        const string OPERATOR_MISSING_FROM_XML = "Operator username not listed in the dataset XML";
                        AppendToText(ErrorMessageForDatabase, OPERATOR_MISSING_FROM_XML);
                        ErrorMessageForUser = OPERATOR_MISSING_FROM_XML;
                        validationErrors.Add(new ValidationError("Operator name not listed in " + sourceDescriptionPrefix, string.Empty));
                    }
                    else
                    {
                        var msg = "Operator username not defined in DMS (or ambiguous): " + mXmlOperatorName;
                        AppendToText(ErrorMessageForDatabase, msg);
                        ErrorMessageForUser = msg;
                        validationErrors.Add(new ValidationError("Operator name not defined in DMS", mXmlOperatorName));
                    }

                    if (!string.IsNullOrWhiteSpace(moveLocPath))
                    {
                        validationErrors.Add(new ValidationError("Dataset trigger file path", moveLocPath));
                    }

                    var errorSolution = mDMSInfoCache.GetDbErrorSolution(ErrorMessageForUser);

                    if (string.IsNullOrWhiteSpace(errorSolution))
                    {
                        ErrorMessageForUser = string.Empty;
                    }
                    else
                    {
                        ErrorMessageForUser = errorSolution;
                    }

                    CacheMail(validationErrors, mXmlOperatorEmail, " - Operator not defined.");

                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_FAILED)
                {
                    if (triggerFile == null)
                    {
                        LogWarning("XML Time validation error " + captureInfo.GetSourceDescription(), true);

                        MainProcess.LogErrorToDatabase("Time validation error for " + captureInfo.GetSourceDescription());
                    }
                    else
                    {
                        var moveLocPath = MoveXmlFile(triggerFile, timeValidationDirectory);

                        LogWarning("XML Time validation error " + moveLocPath, true);
                        MainProcess.LogErrorToDatabase("Time validation error. View details in log at " + GetLogFileSharePath() + " for: " + moveLocPath);

                        var validationErrors = new List<ValidationError>
                        {
                            new("Time validation error", moveLocPath)
                        };

                        CacheMail(validationErrors, mXmlOperatorEmail, " - Time validation error.");
                    }
                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR)
                {
                    if (triggerFile == null)
                    {
                        LogWarning("An error was encountered during the validation process for " + captureInfo.GetSourceDescription(), true);
                    }
                    else
                    {
                        var moveLocPath = MoveXmlFile(triggerFile, failureDirectory);

                        LogWarning("An error was encountered during the validation process, file " + moveLocPath, true);
                    }

                    var validationErrors = new List<ValidationError>
                    {
                        new("XML error encountered during validation process", captureInfo.GetSourceDescription())
                    };

                    CacheMail(validationErrors, mXmlOperatorEmail, " - XML validation error.");
                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE)
                {
                    // Logon failure
                    // If processing an XML trigger file, do not move it
                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR)
                {
                    // Network error
                    // Do not process any more datasets for this instrument
                    // Also, if processing an XML trigger file, do not move it
                    UpdateInstrumentsToSkip(mXmlInstrumentName);
                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_SKIP_INSTRUMENT)
                {
                    LogMessage(" ... skipped since m_InstrumentsToSkip contains " + mXmlInstrumentName);
                    UpdateInstrumentsToSkip(mXmlInstrumentName);
                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_WAIT_FOR_FILES)
                {
                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED)
                {
                    // Size changed
                    // If processing an XML trigger file, do not move it
                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_NO_DATA)
                {
                    string sourceDescription;

                    if (triggerFile == null)
                    {
                        sourceDescription = captureInfo.GetSourceDescription();
                    }
                    else
                    {
                        var moveLocPath = MoveXmlFile(triggerFile, failureDirectory);
                        sourceDescription = string.Format("See file {0}", moveLocPath);
                    }

                    LogWarning("Dataset " + myDataXmlValidation.DatasetName + " not found at " + myDataXmlValidation.SourcePath, true);

                    var validationErrors = new List<ValidationError>();

                    var newError = new ValidationError("Dataset not found on the instrument", sourceDescription);

                    if (string.IsNullOrEmpty(myDataXmlValidation.ErrorMessage))
                    {
                        newError.AdditionalInfo = string.Empty;
                    }
                    else
                    {
                        newError.AdditionalInfo = myDataXmlValidation.ErrorMessage;
                    }

                    validationErrors.Add(newError);

                    ErrorMessageForDatabase = AppendToText(ErrorMessageForDatabase, newError.IssueType);
                    ErrorMessageForDatabase = AppendToText(ErrorMessageForDatabase, newError.AdditionalInfo);

                    ErrorMessageForUser = "The dataset data is not available for capture";

                    var errorSolution = mDMSInfoCache.GetDbErrorSolution(ErrorMessageForUser);

                    if (string.IsNullOrWhiteSpace(errorSolution))
                    {
                        ErrorMessageForUser = string.Empty;
                    }
                    else
                    {
                        ErrorMessageForUser = errorSolution;
                    }

                    CacheMail(validationErrors, mXmlOperatorEmail, " - Dataset not found.");

                    return false;
                }

                if (xmlResult == XMLTimeValidation.XmlValidateStatus.XML_VALIDATE_TRIGGER_FILE_MISSING)
                {
                    // The file is now missing; silently move on
                    return false;
                }

                // xmlResult should be one of the following:
                // XML_VALIDATE_SUCCESS
                // XML_VALIDATE_CONTINUE
                // XML_VALIDATE_SKIP_INSTRUMENT

                return true;
            }
            catch (Exception ex)
            {
                if (captureInfo is TriggerFileInfo xmlTriggerFileInfo)
                {
                    MainProcess.LogErrorToDatabase("Error validating XML data in file " + xmlTriggerFileInfo.TriggerFile.FullName, ex);
                }
                else
                {
                    MainProcess.LogErrorToDatabase("Error validating XML data for " + captureInfo.GetSourceDescription(), ex);
                }

                return false;
            }
        }

        private static void ShowTraceMessage(string message)
        {
            MainProcess.ShowTraceMessage(message);
        }

        private void UpdateErrorMessagePropertiesIfEmpty(string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(ErrorMessageForUser))
            {
                ErrorMessageForUser = errorMessage;
            }

            if (string.IsNullOrWhiteSpace(ErrorMessageForDatabase))
            {
                ErrorMessageForDatabase = errorMessage;
            }
        }
    }
}
