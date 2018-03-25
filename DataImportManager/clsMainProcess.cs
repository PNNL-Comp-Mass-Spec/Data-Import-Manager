using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using PRISM;
using PRISM.Logging;
using PRISM.FileProcessor;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class clsMainProcess : clsLoggerBase
    {
        #region "Constants and enums"

        private const int MAX_ERROR_COUNT = 4;

        internal enum CloseOutType
        {
            // ReSharper disable InconsistentNaming
            CLOSEOUT_SUCCESS = 0,
            CLOSEOUT_FAILED = 1,
            CLOSEOUT_NO_DATA = 10
            // ReSharper restore InconsistentNaming
        }

        #endregion

        #region "Member Variables"

        private clsMgrSettings mMgrSettings;

        private bool mConfigChanged;

        private FileSystemWatcher mFileWatcher;

        private bool mMgrActive = true;

        private int mDebugLevel;

        /// <summary>
        /// Keys in this dictionary are instrument names
        /// Values are the number of datasets skipped for the given instrument
        /// </summary>
        /// <remarks></remarks>
        private readonly ConcurrentDictionary<string, int> mInstrumentsToSkip;

        private int mFailureCount;
        /// <summary>
        /// Keys in this dictionary are semicolon separated e-mail addresses
        /// Values are mail messages to send
        /// </summary>
        /// <remarks></remarks>
        private readonly ConcurrentDictionary<string, ConcurrentBag<clsQueuedMail>> mQueuedMail;

        #endregion

        #region "Properties"

        public bool IgnoreInstrumentSourceErrors { get; set; }
        public bool MailDisabled { get; set; }
        public bool PreviewMode { get; set; }
        public bool TraceMode { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="traceMode"></param>
        public clsMainProcess(bool traceMode)
        {
            TraceMode = traceMode;

            mInstrumentsToSkip = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            mQueuedMail = new ConcurrentDictionary<string, ConcurrentBag<clsQueuedMail>>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Load the manager settings
        /// </summary>
        /// <returns></returns>
        public bool InitMgr()
        {
            try
            {
                mMgrSettings = new clsMgrSettings(TraceMode);
                if (mMgrSettings.ManagerDeactivated)
                {
                    if (TraceMode)
                    {
                        ShowTraceMessage("m_MgrSettings.ManagerDeactivated = True");
                    }

                    return false;
                }

            }
            catch (Exception ex)
            {
                throw new Exception("InitMgr, " + ex.Message, ex);
                // Failures are logged by clsMgrSettings to local emergency log file
            }

            var connectionString = mMgrSettings.GetParam("connectionstring");
            var logFileBaseName = mMgrSettings.GetParam("logfilename");

            try
            {
                // Load initial settings
                mMgrActive = bool.Parse(mMgrSettings.GetParam("mgractive"));
                mDebugLevel = int.Parse(mMgrSettings.GetParam("debuglevel"));

                // Create the object that will manage the logging
                var moduleName = mMgrSettings.GetParam("modulename");
                if (string.IsNullOrWhiteSpace(moduleName))
                {
                    moduleName = "DataImportManager: " + clsGlobal.GetHostName();
                }

                LogTools.CreateFileLogger(logFileBaseName);
                LogTools.CreateDbLogger(connectionString, moduleName);

                // Write the initial log and status entries
                var appVersion = ProcessFilesOrFoldersBase.GetEntryOrExecutingAssembly().GetName().Version;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO,
                    "===== Started Data Import Manager V" + appVersion + " =====");
            }
            catch (Exception ex)
            {
                throw new Exception("InitMgr, " + ex.Message, ex);
            }

            var exeFile = new FileInfo(clsGlobal.GetExePath());

            // Set up the FileWatcher to detect setup file changes
            mFileWatcher = new FileSystemWatcher();
            mFileWatcher.BeginInit();
            mFileWatcher.Path = exeFile.DirectoryName;
            mFileWatcher.IncludeSubdirectories = false;
            mFileWatcher.Filter = mMgrSettings.GetParam("configfilename");
            mFileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
            mFileWatcher.EndInit();
            mFileWatcher.EnableRaisingEvents = true;
            mFileWatcher.Changed += FileWatcher_Changed;

            // Get the debug level
            mDebugLevel = int.Parse(mMgrSettings.GetParam("debuglevel"));
            return true;
        }

        /// <summary>
        /// Look for new XML files to process
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Returns true even if no XML files are found</remarks>
        public bool DoImport()
        {
            try
            {
                // Verify an error hasn't left the the system in an odd state
                if (clsGlobal.DetectStatusFlagFile())
                {
                    LogWarning("Flag file exists - auto-deleting it, then closing program");
                    clsGlobal.DeleteStatusFlagFile();
                    if (!clsGlobal.GetHostName().ToLower().StartsWith("monroe"))
                    {
                        return true;
                    }

                }

                if (clsWindowsUpdateStatus.ServerUpdatesArePending(DateTime.Now, out var pendingWindowsUpdateMessage))
                {
                    var warnMessage = "Monthly windows updates are pending; aborting check for new XML trigger files: " + pendingWindowsUpdateMessage;

                    if (TraceMode)
                    {
                        ShowTraceMessage(warnMessage);
                    }
                    else
                    {
                        Console.WriteLine(warnMessage);
                    }

                    return true;
                }

                // Check to see if machine settings have changed
                if (mConfigChanged)
                {
                    if (TraceMode)
                    {
                        ShowTraceMessage("Loading manager settings from the database");
                    }

                    mConfigChanged = false;
                    if (!mMgrSettings.LoadSettings())
                    {
                        if (!string.IsNullOrEmpty(mMgrSettings.ErrMsg))
                        {
                            // Manager has been deactivated, so report this
                            LogWarning(mMgrSettings.ErrMsg);
                        }
                        else
                        {
                            // Unknown problem reading config file
                            LogError("Unknown error re-reading config file");
                        }

                        return false;
                    }

                    mFileWatcher.EnableRaisingEvents = true;
                }

                // Check to see if excessive consecutive failures have occurred
                if (mFailureCount > MAX_ERROR_COUNT)
                {
                    // More than MAX_ERROR_COUNT consecutive failures; there must be a generic problem, so exit
                    LogError("Excessive task failures, disabling manager");
                    DisableManagerLocally();
                }

                // Check to see if the manager is still active
                if (!mMgrActive)
                {
                    LogMessage("Manager inactive");
                    return false;
                }

                var connectionString = mMgrSettings.GetParam("connectionstring");
                DMSInfoCache infoCache;
                try
                {
                    infoCache = new DMSInfoCache(connectionString, TraceMode);
                }
                catch (Exception ex)
                {
                    LogError("Unable to connect to the database using " + connectionString, ex);
                    return false;
                }

                infoCache.DatabaseErrorEvent += OnDatabaseErrorEvent;

                // Check to see if there are any data import files ready
                DoDataImportTask(infoCache);
                clsProgRunner.SleepMilliseconds(250);

                return true;
            }
            catch (Exception ex)
            {
                LogErrorToDatabase("Exception in clsMainProcess.DoImport()", ex);
                return false;
            }

        }

        private void DoDataImportTask(DMSInfoCache infoCache)
        {
            var delBadXmlFilesDays = int.Parse(mMgrSettings.GetParam("deletebadxmlfiles"));
            var delGoodXmlFilesDays = int.Parse(mMgrSettings.GetParam("deletegoodxmlfiles"));
            var successFolder = mMgrSettings.GetParam("successfolder");
            var failureFolder = mMgrSettings.GetParam("failurefolder");

            try
            {
                var result = ScanXferDirectory(out var xmlFilesToImport);

                if (result == CloseOutType.CLOSEOUT_SUCCESS && xmlFilesToImport.Count > 0)
                {
                    // Set status file for control of future runs
                    clsGlobal.CreateStatusFlagFile();

                    // Add a delay
                    var importDelayText = mMgrSettings.GetParam("importdelay");

                    if (!int.TryParse(importDelayText, out var importDelay))
                    {
                        var statusMsg = "Manager parameter ImportDelay was not numeric: " + importDelayText;
                        LogMessage(statusMsg);
                        importDelay = 2;
                    }

                    if (clsGlobal.GetHostName().ToLower().StartsWith("monroe"))
                    {
                        // Console.WriteLine("Changing importDelay from " & importDelay & " seconds to 1 second since host starts with Monroe")
                        importDelay = 1;
                    }
                    else if (PreviewMode)
                    {
                        // Console.WriteLine("Changing importDelay from " & importDelay & " seconds to 1 second since PreviewMode is enabled")
                        importDelay = 1;
                    }

                    if (TraceMode)
                    {
                        ShowTraceMessage("ImportDelay, sleep for " + importDelay + " seconds");
                    }

                    ConsoleMsgUtils.SleepSeconds(importDelay);

                    // Load information from DMS
                    infoCache.LoadDMSInfo();

                    // Randomize order of files in m_XmlFilesToLoad
                    xmlFilesToImport.Shuffle();
                    if (TraceMode)
                    {
                        ShowTraceMessage("Processing " + xmlFilesToImport.Count + " XML files");
                    }

                    // Process the files in parallel, in groups of 50 at a time
                    //
                    while (xmlFilesToImport.Count > 0)
                    {
                        var currentChunk = GetNextChunk(ref xmlFilesToImport, 50).ToList();

                        var itemCount = currentChunk.Count;
                        if (itemCount > 1)
                        {
                            LogMessage("Processing " + itemCount + " XML files in parallel");
                        }

                        Parallel.ForEach(currentChunk, (currentFile) => ProcessOneFile(currentFile, successFolder, failureFolder, infoCache));
                    }

                }
                else
                {
                    if (mDebugLevel > 4 || TraceMode)
                    {
                        LogDebug("No data files to import");
                    }

                    return;
                }

                // Send any queued mail
                if (mQueuedMail.Count > 0)
                {
                    SendQueuedMail();
                }

                foreach (var kvItem in mInstrumentsToSkip)
                {
                    var strMessage = "Skipped " + kvItem.Value + " dataset";
                    if (kvItem.Value != 1)
                    {
                        strMessage += "s";
                    }
                    strMessage += " for instrument " + kvItem.Key + " due to network errors";
                    LogMessage(strMessage);
                }

                // Remove successful XML files older than x days
                DeleteXmlFiles(successFolder, delGoodXmlFilesDays);

                // Remove failed XML files older than x days
                DeleteXmlFiles(failureFolder, delBadXmlFilesDays);

                // If we got to here, closeout the task as a success
                clsGlobal.DeleteStatusFlagFile();

                mFailureCount = 0;
                LogMessage("Completed task");
            }
            catch (Exception ex)
            {
                mFailureCount++;
                LogError("Exception in clsMainProcess.DoDataImportTask", ex);
            }

        }

        private string GetLogFileSharePath()
        {
            var logFileName = mMgrSettings.GetParam("logfilename");
            return clsProcessXmlTriggerFile.GetLogFileSharePath(logFileName);
        }

        /// <summary>
        /// Retrieve the next chunk of items from a list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sourceList">List of items to retrieve a chunk from; will be updated to remove the items in the returned list</param>
        /// <param name="chunksize">Number of items to return</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private IEnumerable<T> GetNextChunk<T>(ref List<T> sourceList, int chunksize)
        {
            if (chunksize < 1)
            {
                chunksize = 1;
            }

            if (sourceList.Count < 1)
            {
                return new List<T>();
            }

            IEnumerable<T> nextChunk;

            if (chunksize >= sourceList.Count)
            {
                nextChunk = sourceList.Take(sourceList.Count).ToList();
                sourceList = new List<T>();
            }
            else
            {
                nextChunk = sourceList.Take(chunksize).ToList();
                var remainingItems = sourceList.Skip(chunksize);
                sourceList = remainingItems.ToList();
            }

            return nextChunk;
        }

        public static void LogErrorToDatabase(string message, Exception ex = null)
        {
            LogError(message, ex, true);
        }

        private void ProcessOneFile(FileInfo currentFile, string successfolder, string failureFolder, DMSInfoCache infoCache)
        {
            var objRand = new Random();

            // Delay for anywhere between 1 to 15 seconds so that the tasks don't all fire at once
            var waitSeconds = objRand.Next(1, 15);
            ConsoleMsgUtils.SleepSeconds(waitSeconds);

            // Validate the xml file
            var udtSettings = new clsProcessXmlTriggerFile.XmlProcSettingsType
            {
                DebugLevel = mDebugLevel,
                IgnoreInstrumentSourceErrors = IgnoreInstrumentSourceErrors,
                PreviewMode = PreviewMode,
                TraceMode = TraceMode,
                FailureFolder = failureFolder,
                SuccessFolder = successfolder
            };

            var triggerProcessor = new clsProcessXmlTriggerFile(mMgrSettings, mInstrumentsToSkip, infoCache, udtSettings);
            triggerProcessor.ProcessFile(currentFile);
            if (triggerProcessor.QueuedMail.Count > 0)
            {
                AddToMailQueue(triggerProcessor.QueuedMail);
            }

        }

        /// <summary>
        /// Add one or more mail messages to mQueuedMail
        /// </summary>
        /// <param name="newQueuedMail"></param>
        /// <remarks></remarks>
        private void AddToMailQueue(ConcurrentDictionary<string, ConcurrentBag<clsQueuedMail>> newQueuedMail)
        {
            foreach (var newQueuedMessage in newQueuedMail)
            {
                var recipients = newQueuedMessage.Key;

                if (mQueuedMail.TryGetValue(recipients, out var queuedMessages))
                {
                    foreach (var msg in newQueuedMessage.Value)
                    {
                        queuedMessages.Add(msg);
                    }

                    continue;
                }

                if (mQueuedMail.TryAdd(recipients, newQueuedMessage.Value))
                    continue;

                if (mQueuedMail.TryGetValue(recipients, out var queuedMessagesRetry))
                {
                    foreach (var msg in newQueuedMessage.Value)
                    {
                        queuedMessagesRetry.Add(msg);
                    }
                }
            }
        }

        public CloseOutType ScanXferDirectory(out List<FileInfo> xmlFilesToImport)
        {
            // Copies the results to the transfer directory
            var serverXferDir = mMgrSettings.GetParam("xferdir");
            if (string.IsNullOrWhiteSpace(serverXferDir))
            {
                LogErrorToDatabase("Manager parameter xferdir is empty (" + clsGlobal.GetHostName() + ")");
                xmlFilesToImport = new List<FileInfo>();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var diXferDirectory = new DirectoryInfo(serverXferDir);

            // Verify transfer directory exists
            if (!diXferDirectory.Exists)
            {
                // There's a serious problem if the xfer directory can't be found!!!
                LogErrorToDatabase("Xml transfer folder not found: " + serverXferDir);
                xmlFilesToImport = new List<FileInfo>();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Load all the Xml File names and dates in the transfer directory into a string dictionary
            try
            {
                if (TraceMode)
                {
                    ShowTraceMessage("Finding XML files at " + serverXferDir);
                }

                xmlFilesToImport = diXferDirectory.GetFiles("*.xml").ToList();
            }
            catch (Exception ex)
            {
                LogErrorToDatabase("Error loading Xml Data files from " + serverXferDir, ex);
                xmlFilesToImport = new List<FileInfo>();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Everything must be OK if we got to here
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Send one digest e-mail to each unique combination of recipients
        /// </summary>
        /// <remarks>
        /// Use of a digest e-mail reduces the e-mail spam sent by this tool
        /// </remarks>
        private void SendQueuedMail()
        {
            var currentTask = "Initializing";
            try
            {
                currentTask = "Get smptserver param";
                var mailServer = mMgrSettings.GetParam("smtpserver");
                if (string.IsNullOrEmpty(mailServer))
                {
                    LogError("Manager parameter smtpserver is empty; cannot send mail");
                    return;
                }

                currentTask = "Check for new log file";

                var logFileName = "MailLog_" + DateTime.Now.ToString("yyyy-MM") + ".txt";

                FileInfo mailLogFile;
                if (string.IsNullOrWhiteSpace(LogTools.CurrentLogFilePath))
                {
                    var exeDirectoryPath = clsGlobal.GetExeDirectoryPath();
                    mailLogFile = new FileInfo(Path.Combine(exeDirectoryPath, "Logs", logFileName));
                }
                else
                {
                    currentTask = "Get current log file path";
                    var currentLogFile = new FileInfo(LogTools.CurrentLogFilePath);

                    currentTask = "Get new log file path";
                    if (currentLogFile.Directory == null)
                        mailLogFile = new FileInfo(logFileName);
                    else
                        mailLogFile = new FileInfo(Path.Combine(currentLogFile.Directory.FullName, logFileName));
                }

                var newLogFile = !mailLogFile.Exists;

                currentTask = "Initialize stringbuilder";

                var mailContentPreview = new StringBuilder();

                if (TraceMode)
                {
                    if (newLogFile)
                        ShowTraceMessage("Creating new mail log file " + mailLogFile.FullName);
                    else
                        ShowTraceMessage("Appending to mail log file " + mailLogFile.FullName);
                }

                currentTask = "Create the mail logger";
                using (var mailLogger = new StreamWriter(new FileStream(mailLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)))
                {
                    mailLogger.AutoFlush = true;

                    currentTask = "Iterate over mQueuedMail";
                    foreach (var queuedMailContainer in mQueuedMail)
                    {
                        var recipients = queuedMailContainer.Key;
                        var messageCount = queuedMailContainer.Value.Count;

                        if (messageCount < 1)
                        {
                            if (TraceMode)
                            {
                                ShowTraceMessage("Empty clsQueuedMail list; this should never happen");
                            }

                            LogWarning("Empty mail queue for recipients " + recipients + "; nothing to do", true);
                            continue;
                        }

                        currentTask = "Get first queued mail";

                        var firstQueuedMail = queuedMailContainer.Value.First();
                        if (firstQueuedMail == null)
                        {
                            LogErrorToDatabase("firstQueuedMail item is null in SendQueuedMail");

                            var defaultRecipients = mMgrSettings.GetParam("to");
                            firstQueuedMail = new clsQueuedMail("Unknown Operator", defaultRecipients, "Exception", new List<clsValidationError>());
                        }

                        // Create the mail message
                        var mailToSend = new MailMessage
                        {
                            From = new MailAddress(mMgrSettings.GetParam("from"))
                        };

                        var mailRecipientsList = firstQueuedMail.Recipients.Split(';').Distinct().ToList();
                        foreach (var emailAddress in mailRecipientsList)
                        {
                            mailToSend.To.Add(emailAddress);
                        }

                        mailToSend.Subject = firstQueuedMail.Subject;

                        var subjectList = new SortedSet<string>();
                        var databaseErrorMsgs = new SortedSet<string>();
                        var instrumentFilePaths = new SortedSet<string>();

                        var mailBody = new StringBuilder();

                        if (messageCount == 1)
                        {
                            LogDebug("E-mailing " + recipients + " regarding " + firstQueuedMail.InstrumentDatasetPath);
                        }
                        else
                        {
                            LogDebug("E-mailing " + recipients + " regarding " + messageCount + " errors");
                        }

                        if (!string.IsNullOrWhiteSpace(firstQueuedMail.InstrumentOperator))
                        {
                            mailBody.AppendLine("Operator: " + firstQueuedMail.InstrumentOperator);
                            if (messageCount > 1)
                            {
                                mailBody.AppendLine();
                            }

                        }

                        // Summarize the validation errors
                        var summarizedErrors = new Dictionary<string, clsValidationErrorSummary>();
                        var messageNumber = 0;
                        var nextSortWeight = 1;

                        currentTask = "Summarize validation errors in queuedMailContainer.Value";
                        foreach (var queuedMailItem in queuedMailContainer.Value)
                        {
                            if (queuedMailItem == null)
                            {
                                LogErrorToDatabase("queuedMailItem is nothing for " + queuedMailContainer.Key);
                                continue;
                            }

                            messageNumber++;
                            string statusMsg;
                            if (string.IsNullOrWhiteSpace(queuedMailItem.InstrumentDatasetPath))
                            {
                                statusMsg = string.Format("XML File {0}: queuedMailItem.InstrumentDatasetPath is empty", messageNumber);
                            }
                            else
                            {
                                statusMsg = string.Format("XML File {0}: {1}", messageNumber, queuedMailItem.InstrumentDatasetPath);
                                if (!instrumentFilePaths.Contains(queuedMailItem.InstrumentDatasetPath))
                                {
                                    instrumentFilePaths.Add(queuedMailItem.InstrumentDatasetPath);
                                }

                            }

                            LogDebug(statusMsg);
                            currentTask = "Iterate over queuedMailItem.ValidationErrors, message " + messageNumber;
                            foreach (var validationError in queuedMailItem.ValidationErrors)
                            {
                                if (!summarizedErrors.TryGetValue(validationError.IssueType, out var errorSummary))
                                {
                                    errorSummary = new clsValidationErrorSummary(validationError.IssueType, nextSortWeight);
                                    nextSortWeight++;
                                    summarizedErrors.Add(validationError.IssueType, errorSummary);
                                }

                                var affectedItem = new clsValidationErrorSummary.AffectedItemType
                                {
                                    IssueDetail = validationError.IssueDetail,
                                    AdditionalInfo = validationError.AdditionalInfo
                                };
                                errorSummary.AffectedItems.Add(affectedItem);

                                if (string.IsNullOrWhiteSpace(queuedMailItem.DatabaseErrorMsg))
                                    continue;

                                if (databaseErrorMsgs.Contains(queuedMailItem.DatabaseErrorMsg))
                                    continue;

                                databaseErrorMsgs.Add(queuedMailItem.DatabaseErrorMsg);
                                errorSummary.DatabaseErrorMsg = queuedMailItem.DatabaseErrorMsg;

                            } // foreach validationError

                            if (!subjectList.Contains(queuedMailItem.Subject))
                            {
                                subjectList.Add(queuedMailItem.Subject);
                            }

                        } // foreach queuedMailItem

                        currentTask = "Iterate over summarizedErrors, sorted by SortWeight";

                        var additionalInfoList = new List<string>();

                        foreach (var errorEntry in (from item in summarizedErrors orderby item.Value.SortWeight select item))
                        {
                            var errorSummary = errorEntry.Value;

                            var affectedItems = (from item in errorSummary.AffectedItems where !String.IsNullOrWhiteSpace(item.IssueDetail) select item).ToList();

                            if (affectedItems.Count > 0)
                            {
                                mailBody.AppendLine(errorEntry.Key + ": ");

                                foreach (var affectedItem in affectedItems)
                                {
                                    mailBody.AppendLine("  " + affectedItem.IssueDetail);
                                    if (string.IsNullOrWhiteSpace(affectedItem.AdditionalInfo))
                                        continue;

                                    if (!string.Equals(additionalInfoList.LastOrDefault(), affectedItem.AdditionalInfo))
                                    {
                                        additionalInfoList.Add(affectedItem.AdditionalInfo);
                                    }
                                }

                                foreach (var infoItem in additionalInfoList)
                                {
                                    // Add the cached additional info items
                                    mailBody.AppendLine("  " + infoItem);
                                }

                            }
                            else
                            {
                                mailBody.AppendLine(errorEntry.Key);
                            }

                            mailBody.AppendLine();

                            if (string.IsNullOrWhiteSpace(errorSummary.DatabaseErrorMsg))
                                continue;

                            mailBody.AppendLine(errorSummary.DatabaseErrorMsg);
                            mailBody.AppendLine();

                        } // foreach errorEntry

                        if (instrumentFilePaths.Count == 1)
                        {
                            mailBody.AppendLine("Instrument file:" + Environment.NewLine + instrumentFilePaths.First());
                        }
                        else if (instrumentFilePaths.Count > 1)
                        {
                            mailBody.AppendLine("Instrument files:");
                            foreach (var triggerFile in instrumentFilePaths)
                            {
                                mailBody.AppendLine("  " + triggerFile);
                            }
                        }

                        currentTask = "Examine subject";
                        if (subjectList.Count > 1)
                        {
                            // Possibly update the subject of the e-mail
                            // Common subjects:
                            // "Data Import Manager - Database error."
                            //   or
                            // "Data Import Manager - Database warning."
                            //  or
                            // "Data Import Manager - Operator not defined."
                            // If any of the subjects contains "error", use it for the mail subject
                            foreach (var subject in (from item in subjectList where item.ToLower().Contains("error") select item))
                            {
                                mailToSend.Subject = subject;
                                break;
                            }
                        }

                        mailBody.AppendLine();
                        mailBody.AppendLine("Log file location:");
                        mailBody.AppendLine("  " + GetLogFileSharePath());
                        mailBody.AppendLine();
                        mailBody.AppendLine(
                            "This message was sent from an account that is not monitored. " +
                            "If you have any questions, please reply to the list of recipients directly.");

                        if (messageCount > 1)
                        {
                            // Add the message count to the subject, e.g. 3x
                            mailToSend.Subject = string.Format("{0} ({1}x)", mailToSend.Subject, messageCount);
                        }

                        mailToSend.Body = mailBody.ToString();
                        if (MailDisabled)
                        {
                            currentTask = "Cache the mail for preview";
                            mailContentPreview.AppendLine("E-mail that would be sent:");
                            mailContentPreview.AppendLine("To: " + recipients);
                            mailContentPreview.AppendLine("Subject: " + mailToSend.Subject);
                            mailContentPreview.AppendLine();
                            mailContentPreview.AppendLine(mailToSend.Body);
                            mailContentPreview.AppendLine();
                        }
                        else
                        {
                            currentTask = "Send the mail";
                            var smtp = new SmtpClient(mailServer);
                            smtp.Send(mailToSend);

                            clsProgRunner.SleepMilliseconds(100);
                        }

                        if (newLogFile)
                        {
                            newLogFile = false;
                        }
                        else
                        {
                            mailLogger.WriteLine();
                            mailLogger.WriteLine("===============================================");
                        }

                        mailLogger.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt"));
                        mailLogger.WriteLine();
                        mailLogger.WriteLine("To: " + recipients);
                        mailLogger.WriteLine("Subject: " + mailToSend.Subject);
                        mailLogger.WriteLine();
                        mailLogger.WriteLine(mailToSend.Body);

                    } // foreach queuedMailContainer

                    currentTask = "Preview cached messages";
                    if (mailContentPreview.Length > 0)
                    {
                        ShowTraceMessage("Mail content preview" + Environment.NewLine + mailContentPreview);
                    }

                }
            }
            catch (Exception ex)
            {
                var msg = "Error in SendQueuedMail, task " + currentTask;
                LogErrorToDatabase(msg, ex);
                throw new Exception(msg, ex);
            }
        }

        private void FileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            mConfigChanged = true;
            if (mDebugLevel > 3)
            {
                LogDebug("Config file changed");
            }

            mFileWatcher.EnableRaisingEvents = false;
        }

        private void OnDatabaseErrorEvent(string message)
        {
            if (TraceMode)
            {
                ShowTraceMessage("Database error message: " + message);
            }

            LogError(message);
        }

        private void DeleteXmlFiles(string folderPath, int fileAgeDays)
        {
            var workingDirectory = new DirectoryInfo(folderPath);

            // Verify directory exists
            if (!workingDirectory.Exists)
            {
                // There's a serious problem if the success/failure directory can't be found!!!
                LogErrorToDatabase("Xml success/failure folder not found: " + folderPath);
                return;
            }

            var deleteFailureCount = 0;
            try
            {
                var xmlFiles = workingDirectory.GetFiles("*.xml").ToList();

                foreach (var xmlFile in xmlFiles)
                {
                    var filedate = xmlFile.LastWriteTimeUtc;
                    var daysDiff = DateTime.UtcNow.Subtract(filedate).Days;
                    if (daysDiff <= fileAgeDays)
                        continue;

                    if (PreviewMode)
                    {
                        Console.WriteLine("Preview: delete old file: " + xmlFile.FullName);
                        continue;
                    }

                    try
                    {
                        xmlFile.Delete();
                    }
                    catch (Exception ex)
                    {
                        LogError("Error deleting old Xml Data file " + xmlFile.FullName, ex);
                        deleteFailureCount++;
                    }
                }
            }
            catch (Exception ex)
            {
                LogErrorToDatabase("Error deleting old Xml Data files at " + folderPath, ex);
                return;
            }

            if (deleteFailureCount <= 0) return;

            var errMsg = "Error deleting " + deleteFailureCount + " XML files at " + folderPath +
                         " -- for a detailed list, see log file " + GetLogFileSharePath();

            LogErrorToDatabase(errMsg);
        }

        private void DisableManagerLocally()
        {
            if (!mMgrSettings.WriteConfigSetting("MgrActive_Local", "False"))
            {
                LogError("Error while disabling manager: " + mMgrSettings.ErrMsg);
            }

        }

        public static void ShowTraceMessage(string message)
        {
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") + ": " + message);
        }
    }
}
