﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using JetBrains.Annotations;
using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;
using PRISMDatabaseUtils;
using PRISMDatabaseUtils.AppSettings;
using PRISMDatabaseUtils.Logging;
using static DataImportManager.ProcessDatasetInfoBase;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    public class MainProcess : LoggerBase
    {
        // Ignore Spelling: Proteinseqs, smtp, spam, yyyy-MM, yyyy-MM-dd hh:mm:ss tt

        private const string DEFAULT_BASE_LOGFILE_NAME = @"Logs\DataImportManager";

        internal const string DEVELOPER_COMPUTER_NAME = "WE43320";

        private const string MGR_PARAM_MGR_ACTIVE = "MgrActive";

        private const bool PROCESS_IN_PARALLEL = true;

        /// <summary>
        /// Closeout type
        /// </summary>
        public enum CloseOutType
        {
            CLOSEOUT_SUCCESS = 0,
            CLOSEOUT_FAILED = 1
        }

        private MgrSettingsDB mMgrSettings;

        private bool mConfigChanged;

        private FileSystemWatcher mFileWatcher;

        private bool mMgrActive = true;

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>Higher values lead to more log messages</remarks>
        private int mDebugLevel;

        /// <summary>
        /// Keys in this dictionary are instrument names
        /// Values are the number of datasets skipped for the given instrument
        /// </summary>
        private readonly ConcurrentDictionary<string, int> mInstrumentsToSkip;

        /// <summary>
        /// Keys in this dictionary are semicolon separated e-mail addresses
        /// Values are mail messages to send
        /// </summary>
        private readonly ConcurrentDictionary<string, ConcurrentBag<QueuedMail>> mQueuedMail;

        /// <summary>
        /// When true, ignore instrument source errors
        /// </summary>
        public bool IgnoreInstrumentSourceErrors { get; set; }

        /// <summary>
        /// When true, do not send e-mails
        /// </summary>
        public bool MailDisabled { get; set; }

        /// <summary>
        /// When true, preview adding new datasets
        /// </summary>
        public bool PreviewMode { get; set; }

        /// <summary>
        /// When true, show additional messages
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="traceMode">If true, show trace messages</param>
        public MainProcess(bool traceMode)
        {
            TraceMode = traceMode;

            mInstrumentsToSkip = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            mQueuedMail = new ConcurrentDictionary<string, ConcurrentBag<QueuedMail>>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Add one or more mail messages to mQueuedMail
        /// </summary>
        /// <param name="newQueuedMail">Queued mail</param>
        private void AddToMailQueue(ConcurrentDictionary<string, ConcurrentBag<QueuedMail>> newQueuedMail)
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

        /// <summary>
        /// Append text to the string builder, using the given format string and arguments
        /// </summary>
        /// <param name="sb">String builder</param>
        /// <param name="format">Message format string</param>
        /// <param name="args">Arguments to use with formatString</param>
        [StringFormatMethod("format")]
        private void AppendLine(StringBuilder sb, string format, params object[] args)
        {
            sb.AppendFormat(format, args);
            sb.AppendLine();
        }

        /// <summary>
        /// Return an empty string if value is 1; otherwise return "s"
        /// </summary>
        /// <param name="value">Item count</param>
        private static string CheckPlural(int value)
        {
            return value == 1 ? string.Empty : "s";
        }

        /// <summary>
        /// Initializes the database logger in static class PRISM.Logging.LogTools
        /// </summary>
        /// <remarks>Supports both SQL Server and Postgres connection strings</remarks>
        /// <param name="connectionString">Database connection string</param>
        /// <param name="moduleName">Module name used by logger</param>
        /// <param name="traceMode">When true, show additional debug messages at the console</param>
        /// <param name="logLevel">Log threshold level</param>
        private void CreateDbLogger(
            string connectionString,
            string moduleName,
            bool traceMode = false,
            BaseLogger.LogLevels logLevel = BaseLogger.LogLevels.INFO)
        {
            var databaseType = DbToolsFactory.GetServerTypeFromConnectionString(connectionString);

            DatabaseLogger dbLogger = databaseType switch
            {
                DbServerTypes.MSSQLServer => new PRISMDatabaseUtils.Logging.SQLServerDatabaseLogger(),
                DbServerTypes.PostgreSQL => new PostgresDatabaseLogger(),
                _ => throw new Exception("Unsupported database connection string: should be SQL Server or Postgres")
            };

            dbLogger.ChangeConnectionInfo(moduleName, connectionString);

            LogTools.SetDbLogger(dbLogger, logLevel, traceMode);
        }

        private void DeleteXmlFiles(string directoryPath, int fileAgeDays)
        {
            var workingDirectory = new DirectoryInfo(directoryPath);

            // Verify directory exists
            if (!workingDirectory.Exists)
            {
                // There's a serious problem if the success/failure directory can't be found!!!
                LogErrorToDatabase("Xml success/failure directory not found: " + directoryPath);
                return;
            }

            var deleteFailureCount = 0;

            try
            {
                foreach (var xmlFile in workingDirectory.GetFiles("*.xml"))
                {
                    var fileDate = xmlFile.LastWriteTimeUtc;
                    var daysDiff = DateTime.UtcNow.Subtract(fileDate).Days;

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
                LogErrorToDatabase("Error deleting old Xml Data files at " + directoryPath, ex);
                return;
            }

            if (deleteFailureCount == 0)
                return;

            var errMsg = "Error deleting " + deleteFailureCount + " XML files at " + directoryPath +
                         " -- for a detailed list, see log file " + GetLogFileSharePath();

            LogErrorToDatabase(errMsg);
        }

        /// <summary>
        /// Look for new XML files to process; also contact the database to look for any new dataset creation tasks
        /// </summary>
        /// <remarks>Returns true even if no XML files are found</remarks>
        /// <returns>True if success, false if an error</returns>
        public bool DoImport()
        {
            try
            {
                // Verify an error hasn't left the system in an odd state
                if (Global.DetectStatusFlagFile())
                {
                    LogWarning("Flag file exists - auto-deleting it, then closing program");
                    Global.DeleteStatusFlagFile();

                    if (!Global.GetHostName().Equals(DEVELOPER_COMPUTER_NAME, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }

                if (WindowsUpdateStatus.ServerUpdatesArePending(DateTime.Now, out var pendingWindowsUpdateMessage))
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
                    ShowTrace("Loading manager settings from the database");

                    mConfigChanged = false;

                    var localSettings = GetLocalManagerSettings();

                    var success = mMgrSettings.LoadSettings(localSettings, true);

                    if (!success)
                    {
                        if (!string.IsNullOrWhiteSpace(mMgrSettings.ErrMsg))
                        {
                            // Report the error
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

                // Check to see if the manager is active
                if (!mMgrActive)
                {
                    LogMessage("Manager inactive");
                    return false;
                }

                // This connection string points to the DMS database on prismdb2 (previously, DMS5 on Gigasax)
                var connectionString = mMgrSettings.GetParam("ConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrSettings.ManagerName);

                // Example connection strings after adding the application name:
                //   Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;Encrypt=False;Application Name=Proto-6_DIM
                //   Host=prismdb2;Port=5432;Database=dms;Username=svc-dms;Application Name=Proto-6_DIM

                DMSInfoCache infoCache;

                try
                {
                    infoCache = new DMSInfoCache(connectionStringToUse, TraceMode);
                }
                catch (Exception ex)
                {
                    LogError("Unable to connect to the database using " + connectionStringToUse, ex);
                    return false;
                }

                // Check to see if there are any data import files ready
                // Also contact the database to look for any new dataset creation tasks
                ImportNewDatasets(infoCache);

                AppUtils.SleepMilliseconds(250);

                return true;
            }
            catch (Exception ex)
            {
                LogErrorToDatabase("Exception in MainProcess.DoImport()", ex);
                return false;
            }
            finally
            {
                DataImportTask.DisposeSemaphore();
            }
        }

        /// <summary>
        /// Call procedure get_instrument_storage_path_for_new_datasets on all instruments with multiple datasets to add to DMS
        /// </summary>
        /// <param name="xmlParameters">List of XML parameters for dataset create tasksL</param>
        /// <param name="dbTools">DBTools instance</param>
        private void EnsureInstrumentDataStorageDirectories(List<string> xmlParameters, IDBTools dbTools)
        {
            var counts = GetCreateTaskCountsForInstruments(xmlParameters);

            EnsureInstrumentDataStorageDirectories(counts, dbTools);
        }

        /// <summary>
        /// Call procedure get_instrument_storage_path_for_new_datasets on all instruments with multiple datasets to add to DMS
        /// </summary>
        /// <param name="xmlFiles">List of XML trigger files</param>
        /// <param name="dbTools">DBTools instance</param>
        private void EnsureInstrumentDataStorageDirectories(List<FileInfo> xmlFiles, IDBTools dbTools)
        {
            var counts = GetFileCountsForInstruments(xmlFiles);

            EnsureInstrumentDataStorageDirectories(counts, dbTools);
        }

        /// <summary>
        /// Call procedure get_instrument_storage_path_for_new_datasets on all instruments with multiple datasets to add to DMS
        /// </summary>
        /// <remarks>
        /// The purpose is to avoid a race condition in the database that leads to identical single-use entries in T_Storage_Path
        /// </remarks>
        /// <param name="counts">File or create task counts, by instrument</param>
        /// <param name="dbTools">DBTools instance</param>
        private void EnsureInstrumentDataStorageDirectories(Dictionary<string, int> counts, IDBTools dbTools)
        {
            var serverType = DbToolsFactory.GetServerTypeFromConnectionString(dbTools.ConnectStr);

            foreach (var instrument in counts.Where(x => x.Value >= 2).Select(x => x.Key))
            {
                try
                {
                    var query = $"SELECT id FROM V_Instrument_List_Export WHERE name = '{instrument}'";

                    var success = dbTools.GetQueryScalar(query, out var queryResult);

                    if (!success)
                    {
                        LogWarning(string.Format("GetQueryScalar returned false for query: {0}", query));
                        continue;
                    }

                    if (queryResult is not int instrumentId)
                    {
                        LogWarning(string.Format("Query did not return an integer: {0}", query));
                        continue;
                    }

                    if (serverType == DbServerTypes.PostgreSQL)
                    {
                        EnsureInstrumentDataStorageDirectoryPostgres(dbTools, instrumentId, instrument);
                        continue;
                    }

                    EnsureInstrumentDataStorageDirectorySqlServer(dbTools, instrumentId, instrument);
                }
                catch (Exception ex)
                {
                    // If an error occurs, it's likely a database communication error, and this is not a critical step, so just exit the method
                    LogError("MainProcess.EnsureInstrumentDataStorageDirectories(), Error ensuring dataset storage directories", ex, true);
                    return;
                }
            }
        }

        private void EnsureInstrumentDataStorageDirectoryPostgres(IDBTools dbTools, int instrumentId, string instrument)
        {
            const string instrumentStorageFunction = "get_instrument_storage_path_for_new_datasets";

            // Call the storage function, which will create a new storage path if the auto-defined path does not exist in t_storage_path

            var query = string.Format("SELECT * FROM {0}({1}, _refDate => null, _autoSwitchActiveStorage => true) AS storage_path_id", instrumentStorageFunction, instrumentId);

            if (PreviewMode)
            {
                ShowTraceMessage(string.Format(
                    "Preview: run query in database {0}: {1}",
                    dbTools.DatabaseName, query));

                return;
            }

            if (TraceMode)
            {
                ShowTraceMessage(string.Format(
                    "Running query in database {0}: {1}",
                    dbTools.DatabaseName, query));
            }

            // Run the query
            var storagePathSuccess = dbTools.GetQueryScalar(query, out var storagePathID);

            if (!storagePathSuccess)
            {
                LogWarning(string.Format("GetQueryScalar returned false for query: {0}", query));
                return;
            }

            if (TraceMode)
            {
                ShowTraceMessage(string.Format(
                    "Function {0} returned storage path ID {1} for instrument {2}",
                    instrumentStorageFunction, storagePathID, instrument));
            }
        }

        private void EnsureInstrumentDataStorageDirectorySqlServer(IDBTools dbTools, int instrumentId, string instrument)
        {
            const string instrumentStorageProcedure = "get_instrument_storage_path_for_new_datasets";

            // Prepare to call the procedure
            // It will create a new storage path if the auto-defined path does not exist in T_Storage_Path

            var cmd = dbTools.CreateCommand(instrumentStorageProcedure, CommandType.StoredProcedure);
            cmd.CommandTimeout = 45;

            dbTools.AddParameter(cmd, "@instrumentID", SqlType.Int, 1, instrumentId);

            // Set this to null so that the current datetime is used
            dbTools.AddParameter(cmd, "@refDate", SqlType.DateTime).Value = DBNull.Value;

            // SQL Server (defaults to 1)
            dbTools.AddParameter(cmd, "@autoSwitchActiveStorage", SqlType.TinyInt).Value = 1;

            // Postgres (defaults to true)
            // dbTools.AddParameter(cmd, "@autoSwitchActiveStorage", SqlType.Boolean).Value = true;

            // Define parameter for procedure's return value
            // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
            dbTools.AddParameter(cmd, "@return", SqlType.Int, ParameterDirection.ReturnValue);

            if (PreviewMode)
            {
                ShowTraceMessage(string.Format(
                    "Preview: call procedure {0} in database {1}",
                    instrumentStorageProcedure, dbTools.DatabaseName));

                return;
            }

            if (TraceMode)
            {
                ShowTraceMessage(string.Format(
                    "Calling procedure {0} in database {1}",
                    instrumentStorageProcedure, dbTools.DatabaseName));
            }

            // Call the procedure
            var storagePathID = dbTools.ExecuteSP(cmd);

            if (TraceMode)
            {
                ShowTraceMessage(string.Format(
                    "Procedure {0}: returned storage path ID {1} for instrument {2}",
                    instrumentStorageProcedure, storagePathID, instrument));
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

        /// <summary>
        /// Look for instrument names in dataset create task XML parameters and determine the number of datasets to be created for each instrument
        /// </summary>
        /// <param name="xmlParameters">List of XML parameters for dataset create tasks</param>
        /// <returns>Dictionary where keys are instrument names and values are dataset counts</returns>
        public static Dictionary<string, int> GetCreateTaskCountsForInstruments(List<string> xmlParameters)
        {
            var instrumentNames = new List<string>();

            // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
            foreach (var item in xmlParameters)
            {
                var instrument = GetInstrumentFromDatasetCreateTaskParameters(item);
                instrumentNames.Add(instrument);
            }

            return GetDatasetsCountsByInstrument(instrumentNames);
        }

        /// <summary>
        /// Look for instrument names in XML trigger files and determine the number of datasets to be created for each instrument
        /// </summary>
        /// <param name="xmlFiles">List of XML trigger file</param>
        /// <returns>Dictionary where keys are instrument names and values are dataset counts</returns>
        public static Dictionary<string, int> GetFileCountsForInstruments(List<FileInfo> xmlFiles)
        {
            var instrumentNames = new List<string>();

            // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
            foreach (var file in xmlFiles)
            {
                var instrument = GetInstrumentFromXmlFile(file);
                instrumentNames.Add(instrument);
            }

            return GetDatasetsCountsByInstrument(instrumentNames);
        }

        /// <summary>
        /// Determine the number of datasets to be created for each instrument
        /// </summary>
        /// <param name="instrumentNames">List of instrument names</param>
        /// <returns>Dictionary where keys are instrument names and values are dataset counts</returns>
        private static Dictionary<string, int> GetDatasetsCountsByInstrument(List<string> instrumentNames)
        {
            var counts = new Dictionary<string, int>();

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var instrument in instrumentNames)
            {
                if (string.IsNullOrWhiteSpace(instrument))
                    continue;

                if (!counts.ContainsKey(instrument))
                {
                    counts.Add(instrument, 0);
                }

                counts[instrument]++;
            }

            return counts;
        }

        private static string GetInstrumentFromDatasetCreateTaskParameters(string xmlParameters)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(xmlParameters))
                {
                    return string.Empty;
                }

                var doc = XDocument.Parse(xmlParameters);

                // Determine the instrument name by looking for the following XML node

                // <root>
                //   <instrument>Lumos03</instrument>

                foreach (var element in doc.Elements("root").Elements("instrument"))
                {
                    return string.IsNullOrWhiteSpace(element.Value) ? string.Empty : element.Value;
                }

                return string.Empty;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Error in GetInstrumentFromDatasetCreateTaskParameters: {0}", ex.Message));
                return string.Empty;
            }
        }

        private static string GetInstrumentFromXmlFile(FileSystemInfo file)
        {
            try
            {
                if (file?.Exists != true)
                {
                    return string.Empty;
                }

                var doc = XDocument.Load(file.FullName);

                // Determine the instrument name by looking for the following XML node

                // <Dataset>
                //   <Parameter Name="Instrument Name" Value="Lumos03" />

                foreach (var element in doc.Elements("Dataset").Elements("Parameter").Where(el => (string)el.Attribute("Name") == "Instrument Name"))
                {
                    var instrument = element.Attribute("Value")?.Value;

                    return string.IsNullOrWhiteSpace(instrument) ? string.Empty : instrument;
                }

                return string.Empty;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Error in GetInstrumentFromXmlFile: {0}", ex.Message));
                return string.Empty;
            }
        }

        private Dictionary<string, string> GetLocalManagerSettings()
        {
            var defaultSettings = new Dictionary<string, string>
            {
                {MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING, Properties.Settings.Default.MgrCnfgDbConnectStr},
                {MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, Properties.Settings.Default.MgrActive_Local.ToString()},
                {MgrSettings.MGR_PARAM_MGR_NAME, Properties.Settings.Default.MgrName},
                {MgrSettings.MGR_PARAM_USING_DEFAULTS, Properties.Settings.Default.UsingDefaults.ToString()}
            };

            var mgrExePath = AppUtils.GetAppPath();
            var localSettings = mMgrSettings.LoadMgrSettingsFromFile(mgrExePath + ".config");

            if (localSettings == null)
            {
                localSettings = defaultSettings;
            }
            else
            {
                // Make sure the default settings exist and have valid values
                foreach (var setting in defaultSettings)
                {
                    if (!localSettings.TryGetValue(setting.Key, out var existingValue) ||
                        string.IsNullOrWhiteSpace(existingValue))
                    {
                        localSettings[setting.Key] = setting.Value;
                    }
                }
            }

            return localSettings;
        }

        private string GetLogFileSharePath()
        {
            var logFileName = mMgrSettings.GetParam("LogFileName");
            return ProcessDatasetInfoBase.GetLogFileSharePath(logFileName);
        }

        /// <summary>
        /// Retrieve the next chunk of items from a list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sourceList">List of items to retrieve a chunk from; will be updated to remove the items in the returned list</param>
        /// <param name="chunkSize">Number of items to return</param>
        private IEnumerable<T> GetNextChunk<T>(ref List<T> sourceList, int chunkSize)
        {
            if (chunkSize < 1)
            {
                chunkSize = 1;
            }

            if (sourceList.Count < 1)
            {
                return new List<T>();
            }

            IEnumerable<T> nextChunk;

            if (chunkSize >= sourceList.Count)
            {
                nextChunk = sourceList.Take(sourceList.Count).ToList();
                sourceList = new List<T>();
            }
            else
            {
                nextChunk = sourceList.Take(chunkSize).ToList();
                var remainingItems = sourceList.Skip(chunkSize);
                sourceList = remainingItems.ToList();
            }

            return nextChunk;
        }

        private XmlProcSettingsType GetProcessingSettings(string failureDirectory = "", string successDirectory = "")
        {
            return new XmlProcSettingsType
            {
                DebugLevel = mDebugLevel,
                IgnoreInstrumentSourceErrors = IgnoreInstrumentSourceErrors,
                PreviewMode = PreviewMode,
                TraceMode = TraceMode,
                FailureDirectory = failureDirectory,
                SuccessDirectory = successDirectory
            };
        }

        /// <summary>
        /// Extract the value MgrCnfgDbConnectStr from DataImportManager.exe.config
        /// </summary>
        private string GetXmlConfigDefaultConnectionString()
        {
            return GetXmlConfigFileSetting("MgrCnfgDbConnectStr");
        }

        /// <summary>
        /// Extract the value for the given setting from DataImportManager.exe.config
        /// </summary>
        /// <remarks>Uses a simple text reader in case the file has malformed XML</remarks>
        /// <returns>Setting value if found, otherwise an empty string</returns>
        private string GetXmlConfigFileSetting(string settingName)
        {
            if (string.IsNullOrWhiteSpace(settingName))
                throw new ArgumentException("Setting name cannot be blank", nameof(settingName));

            var exePath = Global.GetExePath();

            var configFilePaths = new List<string>();

            if (settingName.Equals("MgrCnfgDbConnectStr", StringComparison.OrdinalIgnoreCase) ||
                settingName.Equals("DefaultDMSConnString", StringComparison.OrdinalIgnoreCase))
            {
                configFilePaths.Add(Path.ChangeExtension(exePath, ".exe.db.config"));
            }

            configFilePaths.Add(Global.GetExePath() + ".config");

            var mgrSettings = new MgrSettings();
            RegisterEvents(mgrSettings);

            var valueFound = mgrSettings.GetXmlConfigFileSetting(configFilePaths, settingName, out var settingValue);
            return valueFound ? settingValue : string.Empty;
        }

        private void ImportNewDatasets(DMSInfoCache infoCache)
        {
            try
            {
                // Create the flag file that indicates that the manager is actively adding new datasets to the database
                Global.CreateStatusFlagFile();

                ProcessXmlTriggerFiles(infoCache);

                ProcessDatasetCreateTasks(infoCache);

                // Send any queued mail
                if (mQueuedMail.Count > 0)
                {
                    SendQueuedMail();
                }

                // If we got to here, delete the status flag file and exit the method
                Global.DeleteStatusFlagFile();
            }
            catch (Exception ex)
            {
                LogError("Exception in MainProcess.ImportNewDatasets", ex);
            }
        }

        /// <summary>
        /// Load the manager settings
        /// </summary>
        public bool InitMgr()
        {
            var defaultModuleName = "DataImportManager: " + Global.GetHostName();

            try
            {
                // Define the default logging info
                // This will get updated below
                LogTools.CreateFileLogger(DEFAULT_BASE_LOGFILE_NAME);

                // Create a database logger connected to the DMS database on prismdb2 (previously, Manager_Control on Proteinseqs)

                // Once the initial parameters have been successfully read,
                // we update the dbLogger to use the connection string read from the Manager Control DB
                string dmsConnectionString;

                // Open DataImportManager.exe.config to look for setting MgrCnfgDbConnectStr, so we know which server to log to by default
                var dmsConnectionStringFromConfig = GetXmlConfigDefaultConnectionString();

                if (string.IsNullOrWhiteSpace(dmsConnectionStringFromConfig))
                {
                    // Use the hard-coded default that points to Proteinseqs
                    dmsConnectionString = Properties.Settings.Default.MgrCnfgDbConnectStr;
                }
                else
                {
                    // Use the connection string from DataImportManager.exe.config
                    dmsConnectionString = dmsConnectionStringFromConfig;
                }

                var managerName = "Data_Import_Manager_" + Global.GetHostName();

                var dbLoggerConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, managerName);

                ShowTrace("Instantiate a DbLogger using " + dbLoggerConnectionString);

                CreateDbLogger(dbLoggerConnectionString, managerName, TraceMode);

                mMgrSettings = new MgrSettingsDB
                {
                    TraceMode = TraceMode
                };

                RegisterEvents(mMgrSettings);
                mMgrSettings.CriticalErrorEvent += ErrorEventHandler;

                var localSettings = GetLocalManagerSettings();

                Console.WriteLine();
                mMgrSettings.ValidatePgPass(localSettings);

                var success = mMgrSettings.LoadSettings(localSettings, true);

                if (!success)
                {
                    if (string.Equals(mMgrSettings.ErrMsg, MgrSettings.DEACTIVATED_LOCALLY))
                        throw new ApplicationException(MgrSettings.DEACTIVATED_LOCALLY);

                    throw new ApplicationException("Unable to initialize manager settings class: " + mMgrSettings.ErrMsg);
                }

                var mgrActiveLocal = mMgrSettings.GetParam(MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, false);

                if (!mgrActiveLocal)
                {
                    ShowTrace(MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL + " is false in the .exe.config file");
                    return false;
                }

                var mgrActive = mMgrSettings.GetParam(MGR_PARAM_MGR_ACTIVE, false);

                if (!mgrActive)
                {
                    ShowTrace("Manager parameter " + MGR_PARAM_MGR_ACTIVE + " is false");
                    return false;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("InitMgr, " + ex.Message, ex);
            }

            // This connection string points to the DMS database on prismdb2 (previously, DMS5 on Gigasax)
            var connectionString = mMgrSettings.GetParam("ConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrSettings.ManagerName);

            var logFileBaseName = mMgrSettings.GetParam("LogFileName");

            try
            {
                // Load initial settings
                mMgrActive = mMgrSettings.GetParam(MGR_PARAM_MGR_ACTIVE, false);
                mDebugLevel = mMgrSettings.GetParam("DebugLevel", 2);

                // Create the object that will manage the logging
                var moduleName = mMgrSettings.GetParam("ModuleName", defaultModuleName);

                LogTools.CreateFileLogger(logFileBaseName);
                CreateDbLogger(connectionStringToUse, moduleName);

                // Write the initial log and status entries
                var appVersion = AppUtils.GetEntryOrExecutingAssembly().GetName().Version;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO,
                    "===== Started Data Import Manager V" + appVersion + " =====");
            }
            catch (Exception ex)
            {
                throw new Exception("InitMgr, " + ex.Message, ex);
            }

            var exeFile = new FileInfo(Global.GetExePath());

            // Set up the FileWatcher to detect setup file changes
            mFileWatcher = new FileSystemWatcher();
            mFileWatcher.BeginInit();
            mFileWatcher.Path = exeFile.DirectoryName;
            mFileWatcher.IncludeSubdirectories = false;
            mFileWatcher.Filter = mMgrSettings.GetParam("ConfigFileName");
            mFileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
            mFileWatcher.EndInit();
            mFileWatcher.EnableRaisingEvents = true;
            mFileWatcher.Changed += FileWatcher_Changed;

            // Get the debug level
            mDebugLevel = mMgrSettings.GetParam("DebugLevel", 2);
            return true;
        }

        /// <summary>
        /// Log an error message to the database
        /// </summary>
        /// <param name="message">Error message</param>
        /// <param name="ex">Exception</param>
        public static void LogErrorToDatabase(string message, Exception ex = null)
        {
            LogError(message, ex, true);
        }

        private void LogProcedureCallError(int resCode, int returnCode, IDataParameter messageParam, IDataParameter returnCodeParam, string procedureName)
        {
            var errorMessage = resCode != 0 && returnCode == 0
                ? string.Format("ExecuteSP() reported result code {0} calling {1}", resCode, procedureName)
                : string.Format("{0} reported return code {1}", procedureName, returnCodeParam.Value.CastDBVal<string>());

            var message = messageParam.Value.CastDBVal<string>();

            if (!string.IsNullOrWhiteSpace(message))
            {
                LogError(errorMessage + "; message: " + message);
            }
            else
            {
                LogError(errorMessage);
            }
        }

        private void NotifySkippedDatasets()
        {
            foreach (var kvItem in mInstrumentsToSkip)
            {
                var message = "Skipped " + kvItem.Value + " dataset";

                if (kvItem.Value != 1)
                {
                    message += "s";
                }

                message += " for instrument " + kvItem.Key + " due to network errors";
                LogMessage(message);
            }
        }

        private void ProcessOneDatasetCreateTask(int entryId, string xmlParameters, DMSInfoCache infoCache)
        {
            const string SET_TASK_COMPLETE_SP = "set_dataset_create_task_complete";

            var settings = GetProcessingSettings();
            var triggerProcessor = new ProcessDatasetCreateTask(mMgrSettings, mInstrumentsToSkip, infoCache, settings);

            triggerProcessor.ProcessXmlParameters(entryId, xmlParameters);

            if (triggerProcessor.QueuedMail.Count > 0)
            {
                AddToMailQueue(triggerProcessor.QueuedMail);
            }

            // Call procedure set_dataset_create_task_complete

            var dbTools = infoCache.DBTools;

            var cmd = dbTools.CreateCommand(SET_TASK_COMPLETE_SP, CommandType.StoredProcedure);

            dbTools.AddParameter(cmd, "@entryID", SqlType.Int).Value = entryId;
            var completionCodeParam = dbTools.AddParameter(cmd, "@completionCode", SqlType.Int);
            var completionMessageParam = dbTools.AddParameter(cmd, "@completionMessage", SqlType.VarChar, 2048);
            var messageParam = dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

            // Define parameter for procedure's return value
            // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
            var returnCodeParam = dbTools.AddParameter(cmd, "@return", SqlType.Int, ParameterDirection.ReturnValue);

            if (string.IsNullOrWhiteSpace(triggerProcessor.ErrorMessageForDatabase))
            {
                if (PreviewMode)
                {
                    // Use completion code -1 to change the dataset create task's state back to 1
                    completionCodeParam.Value = -1;
                }
                else
                {
                    completionCodeParam.Value = 0;
                }

                completionMessageParam.Value = string.Empty;
            }
            else
            {
                completionCodeParam.Value = 1;
                completionMessageParam.Value = triggerProcessor.ErrorMessageForDatabase;
            }

            if (mDebugLevel > 4 || TraceMode)
            {
                LogDebug("Calling procedure " + SET_TASK_COMPLETE_SP);
            }

            // Call the procedure
            var resCode = dbTools.ExecuteSP(cmd);

            var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

            if (resCode == 0 && returnCode == 0)
                return;

            LogProcedureCallError(resCode, returnCode, messageParam, returnCodeParam, SET_TASK_COMPLETE_SP);
        }

        private void ProcessOneFile(FileInfo currentFile, string successDirectory, string failureDirectory, DMSInfoCache infoCache)
        {
            if (PROCESS_IN_PARALLEL)
            {
                // Delay for anywhere between 1 and 15 seconds so that the tasks don't all fire at once
                var objRand = new Random();
                var waitSeconds = objRand.Next(1, 15);
                ConsoleMsgUtils.SleepSeconds(waitSeconds);
            }

            var settings = GetProcessingSettings(failureDirectory, successDirectory);
            var triggerProcessor = new ProcessXmlTriggerFile(mMgrSettings, mInstrumentsToSkip, infoCache, settings);

            triggerProcessor.ProcessFile(currentFile);

            if (triggerProcessor.QueuedMail.Count > 0)
            {
                AddToMailQueue(triggerProcessor.QueuedMail);
            }
        }

        private void ProcessDatasetCreateTasks(DMSInfoCache infoCache)
        {
            const string REQUEST_DATASET_CREATE_TASK_SP = "request_dataset_create_task";

            try
            {
                if (mDebugLevel > 4 || TraceMode)
                {
                    LogDebug("Looking for pending dataset creation tasks");
                }

                var dbTools = infoCache.DBTools;

                var datasetCreateTasks = new Dictionary<int, string>();

                while (true)
                {
                    var cmd = dbTools.CreateCommand(REQUEST_DATASET_CREATE_TASK_SP, CommandType.StoredProcedure);

                    dbTools.AddParameter(cmd, "@processorName", SqlType.VarChar, 128);

                    var entryIdParam = dbTools.AddParameter(cmd, "@entryID", SqlType.Int, ParameterDirection.InputOutput);
                    var parametersParam = dbTools.AddParameter(cmd, "@parameters", SqlType.VarChar, 4000, ParameterDirection.InputOutput);
                    var messageParam = dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

                    // Define parameter for procedure's return value
                    // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
                    var returnCodeParam = dbTools.AddParameter(cmd, "@return", SqlType.Int, ParameterDirection.ReturnValue);

                    if (mDebugLevel > 4 || TraceMode)
                    {
                        LogDebug("Calling procedure " + REQUEST_DATASET_CREATE_TASK_SP);
                    }

                    // Call the procedure
                    var resCode = dbTools.ExecuteSP(cmd);

                    var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

                    if (resCode != 0 || returnCode != 0)
                    {
                        LogProcedureCallError(resCode, returnCode, messageParam, returnCodeParam, REQUEST_DATASET_CREATE_TASK_SP);
                        return;
                    }

                    var entryId = entryIdParam.Value.CastDBVal<int>();

                    if (entryId == 0)
                    {
                        // Exit the while loop
                        break;
                    }

                    var xmlParameters = parametersParam.Value.CastDBVal<string>();

                    if (datasetCreateTasks.ContainsKey(entryId))
                    {
                        LogError(string.Format("Procedure {0} returned entry ID {1} on successive calls, indicating an error", REQUEST_DATASET_CREATE_TASK_SP, entryId));
                        return;
                    }

                    datasetCreateTasks.Add(entryId, xmlParameters);
                }

                if (datasetCreateTasks.Count == 0)
                {
                    if (mDebugLevel > 4 || TraceMode)
                    {
                        LogDebug("No dataset creation tasks were found");
                    }

                    return;
                }

                if (!infoCache.DMSInfoLoaded)
                {
                    // Load information from DMS
                    infoCache.LoadDMSInfo();
                }

                // Populate a list with the dataset creation task IDs so that we can process them in a random order
                var entryIds = datasetCreateTasks.Keys.ToList();

                entryIds.Shuffle();

                ShowTrace(string.Format("Processing {0} dataset create task{1}", entryIds.Count, CheckPlural(entryIds.Count)));

                var currentChunk = new Dictionary<int, string>();

                // Process the dataset create tasks in parallel, in groups of 50 at a time
                while (entryIds.Count > 0)
                {
                    // Call GetNextChunk() to obtain the next 50 items from entryIds (items in currentChunkIDs will no longer exist in List entryIds)
                    var currentChunkIDs = GetNextChunk(ref entryIds, 50).ToList();

                    currentChunk.Clear();

                    foreach (var entryId in currentChunkIDs)
                    {
                        currentChunk.Add(entryId, datasetCreateTasks[entryId]);
                    }

                    var itemCount = currentChunk.Count;

                    if (itemCount > 1)
                    {
                        LogMessage("Processing " + itemCount + " dataset create tasks in parallel");

                        // Use EnsureInstrumentDataStorageDirectories to prevent duplicate entries in T_Storage_Path due to a race condition

                        var currentChunkXmlParameters = new List<string>();

                        // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                        foreach (var item in currentChunk)
                        {
                            currentChunkXmlParameters.Add(item.Value);
                        }

                        EnsureInstrumentDataStorageDirectories(currentChunkXmlParameters, infoCache.DBTools);
                    }

                    if (PROCESS_IN_PARALLEL)
                    {
                        Parallel.ForEach(currentChunk, (currentTask) => ProcessOneDatasetCreateTask(currentTask.Key, currentTask.Value, infoCache));
                    }
                    else
#pragma warning disable CS0162 // Unreachable code detected
                    {
                        // ReSharper disable once HeuristicUnreachableCode
                        foreach (var currentTask in currentChunk)
                        {
                            ProcessOneDatasetCreateTask(currentTask.Key, currentTask.Value, infoCache);
                        }
                    }
#pragma warning restore CS0162 // Unreachable code detected
                }

                NotifySkippedDatasets();

                LogMessage("Done processing dataset create tasks");
            }
            catch (Exception ex)
            {
                LogError("Exception in MainProcess.ProcessDatasetCreateTasks", ex);
            }
        }

        private void ProcessXmlTriggerFiles(DMSInfoCache infoCache)
        {
            try
            {
                var delBadXmlFilesDays = Math.Max(7, mMgrSettings.GetParam("DeleteBadXmlFiles", 180));
                var delGoodXmlFilesDays = Math.Max(7, mMgrSettings.GetParam("DeleteGoodXmlFiles", 30));
                var successDirectory = mMgrSettings.GetParam("SuccessFolder");
                var failureDirectory = mMgrSettings.GetParam("FailureFolder");

                var result = ScanXmlTriggerFileDirectory(out var xmlFilesToImport);

                if (result != CloseOutType.CLOSEOUT_SUCCESS || xmlFilesToImport.Count == 0)
                {
                    if (mDebugLevel > 4 || TraceMode)
                    {
                        LogDebug("No data files to import");
                    }

                    return;
                }

                // Add a delay
                var importDelayText = mMgrSettings.GetParam("ImportDelay");

                if (!int.TryParse(importDelayText, out var importDelay))
                {
                    var statusMsg = "Manager parameter ImportDelay was not numeric: " + importDelayText;
                    LogMessage(statusMsg);
                    importDelay = 2;
                }

                if (Global.GetHostName().Equals(DEVELOPER_COMPUTER_NAME, StringComparison.OrdinalIgnoreCase))
                {
                    // Console.WriteLine("Changing importDelay from " & importDelay & " seconds to 1 second since host is {0}", DEVELOPER_COMPUTER_NAME)
                    importDelay = 1;
                }
                else if (PreviewMode)
                {
                    // Console.WriteLine("Changing importDelay from " & importDelay & " seconds to 1 second since PreviewMode is enabled")
                    importDelay = 1;
                }

                ShowTrace(string.Format("ImportDelay, sleep for {0} second{1}", importDelay, CheckPlural(importDelay)));
                ConsoleMsgUtils.SleepSeconds(importDelay);

                // Load information from DMS
                infoCache.LoadDMSInfo();

                // Randomize order of files in m_XmlFilesToLoad
                xmlFilesToImport.Shuffle();
                ShowTrace(string.Format("Processing {0} XML file{1}", xmlFilesToImport.Count, CheckPlural(xmlFilesToImport.Count)));

                // Process the files in parallel, in groups of 50 at a time
                while (xmlFilesToImport.Count > 0)
                {
                    var currentChunk = GetNextChunk(ref xmlFilesToImport, 50).ToList();

                    var itemCount = currentChunk.Count;

                    if (itemCount > 1)
                    {
                        LogMessage("Processing " + itemCount + " XML files in parallel");

                        // Prevent duplicate entries in T_Storage_Path due to a race condition
                        EnsureInstrumentDataStorageDirectories(currentChunk, infoCache.DBTools);
                    }

                    if (PROCESS_IN_PARALLEL)
                    {
                        Parallel.ForEach(currentChunk, (currentFile) => ProcessOneFile(currentFile, successDirectory, failureDirectory, infoCache));
                    }
                    else
#pragma warning disable CS0162 // Unreachable code detected
                    {
                        // ReSharper disable once HeuristicUnreachableCode
                        foreach (var currentFile in currentChunk)
                        {
                            ProcessOneFile(currentFile, successDirectory, failureDirectory, infoCache);
                        }
                    }
#pragma warning restore CS0162 // Unreachable code detected
                }

                NotifySkippedDatasets();

                // Remove successful XML files older than x days
                DeleteXmlFiles(successDirectory, delGoodXmlFilesDays);

                // Remove failed XML files older than x days
                DeleteXmlFiles(failureDirectory, delBadXmlFilesDays);

                LogMessage("Done processing XML files");
            }
            catch (Exception ex)
            {
                LogError("Exception in MainProcess.ProcessXmlTriggerFiles", ex);
            }
        }

        /// <summary>
        /// Look for available XML trigger files
        /// </summary>
        /// <param name="xmlFilesToImport">List of XML trigger files</param>
        /// <returns>CloseOutType.CLOSEOUT_SUCCESS if successful, CloseOutType.CLOSEOUT_FAILED if an error</returns>
        public CloseOutType ScanXmlTriggerFileDirectory(out List<FileInfo> xmlFilesToImport)
        {
            var triggerFileDirectoryPath = mMgrSettings.GetParam("xferDir");

            if (string.IsNullOrWhiteSpace(triggerFileDirectoryPath))
            {
                LogErrorToDatabase("Manager parameter xferDir is empty (" + Global.GetHostName() + ")");
                xmlFilesToImport = new List<FileInfo>();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var triggerFileDirectory = new DirectoryInfo(triggerFileDirectoryPath);

            // Verify that the directory exists
            if (!triggerFileDirectory.Exists)
            {
                // There's a serious problem if the directory can't be found
                LogErrorToDatabase("XML trigger file directory not found: " + triggerFileDirectoryPath);
                xmlFilesToImport = new List<FileInfo>();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Load all the XML file names and dates in the trigger file directory into a list
            try
            {
                ShowTrace("Finding XML files at " + triggerFileDirectoryPath);

                xmlFilesToImport = triggerFileDirectory.GetFiles("*.xml").ToList();
            }
            catch (Exception ex)
            {
                LogErrorToDatabase("Error loading XML data files from " + triggerFileDirectoryPath, ex);
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
                currentTask = "Get SmtpServer param";

                var mailServer = mMgrSettings.GetParam("SmtpServer");

                if (string.IsNullOrWhiteSpace(mailServer))
                {
                    LogError("Manager parameter SmtpServer is empty; cannot send mail");
                    return;
                }

                currentTask = "Check for new log file";

                var logFileName = "MailLog_" + DateTime.Now.ToString("yyyy-MM") + ".txt";

                FileInfo mailLogFile;

                if (string.IsNullOrWhiteSpace(LogTools.CurrentLogFilePath))
                {
                    var exeDirectoryPath = Global.GetExeDirectoryPath();
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

                currentTask = "Initialize StringBuilder";

                var mailContentPreview = new StringBuilder();

                if (newLogFile)
                    ShowTrace("Creating new mail log file " + mailLogFile.FullName);
                else
                    ShowTrace("Appending to mail log file " + mailLogFile.FullName);

                currentTask = "Create the mail logger";

                using var mailLogger = new StreamWriter(new FileStream(mailLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));

                mailLogger.AutoFlush = true;

                currentTask = "Iterate over mQueuedMail";

                foreach (var queuedMailContainer in mQueuedMail)
                {
                    var recipients = queuedMailContainer.Key;
                    var messageCount = queuedMailContainer.Value.Count;

                    if (messageCount < 1)
                    {
                        ShowTrace("Empty QueuedMail list; this should never happen");

                        LogWarning("Empty mail queue for recipients " + recipients + "; nothing to do", true);
                        continue;
                    }

                    currentTask = "Get first queued mail";

                    var firstQueuedMail = queuedMailContainer.Value.First();

                    if (firstQueuedMail == null)
                    {
                        LogErrorToDatabase("firstQueuedMail item is null in SendQueuedMail");

                        var defaultRecipients = mMgrSettings.GetParam("to");
                        firstQueuedMail = new QueuedMail("Unknown Operator", defaultRecipients, "Exception", new List<ValidationError>());
                    }

                    // Create the mail message
                    var mailToSend = new MailMessage
                    {
                        From = new MailAddress(mMgrSettings.GetParam("from"))
                    };

                    foreach (var emailAddress in firstQueuedMail.Recipients.Split(';').Distinct().ToList())
                    {
                        mailToSend.To.Add(emailAddress);
                    }

                    mailToSend.Subject = firstQueuedMail.Subject;

                    var subjectList = new SortedSet<string>();
                    var databaseErrorMessages = new SortedSet<string>();
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
                        AppendLine(mailBody, "Operator: {0}", firstQueuedMail.InstrumentOperator);

                        if (messageCount > 1)
                        {
                            mailBody.AppendLine();
                        }
                    }

                    // Summarize the validation errors
                    var summarizedErrors = new Dictionary<string, ValidationErrorSummary>();
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

                            // Add the instrument dataset path (if not yet present)
                            instrumentFilePaths.Add(queuedMailItem.InstrumentDatasetPath);
                        }

                        LogDebug(statusMsg);
                        currentTask = "Iterate over queuedMailItem.ValidationErrors, message " + messageNumber;

                        foreach (var validationError in queuedMailItem.ValidationErrors)
                        {
                            if (!summarizedErrors.TryGetValue(validationError.IssueType, out var errorSummary))
                            {
                                errorSummary = new ValidationErrorSummary(validationError.IssueType, nextSortWeight);
                                nextSortWeight++;
                                summarizedErrors.Add(validationError.IssueType, errorSummary);
                            }

                            var affectedItem = new ValidationErrorSummary.AffectedItemType
                            {
                                IssueDetail = validationError.IssueDetail,
                                AdditionalInfo = validationError.AdditionalInfo
                            };

                            errorSummary.AffectedItems.Add(affectedItem);

                            if (string.IsNullOrWhiteSpace(queuedMailItem.ErrorMessageForUser))
                                continue;

                            // ReSharper disable once CanSimplifySetAddingWithSingleCall
                            if (databaseErrorMessages.Contains(queuedMailItem.ErrorMessageForUser))
                                continue;

                            databaseErrorMessages.Add(queuedMailItem.ErrorMessageForUser);
                            errorSummary.DatabaseErrorMsg = queuedMailItem.ErrorMessageForUser;
                        }

                        subjectList.Add(queuedMailItem.Subject);
                    }

                    currentTask = "Iterate over summarizedErrors, sorted by SortWeight";

                    var additionalInfoList = new List<string>();

                    foreach (var errorEntry in (from item in summarizedErrors orderby item.Value.SortWeight select item))
                    {
                        var errorSummary = errorEntry.Value;

                        var affectedItems = (from item in errorSummary.AffectedItems where !String.IsNullOrWhiteSpace(item.IssueDetail) select item).ToList();

                        if (affectedItems.Count > 0)
                        {
                            AppendLine(mailBody, "{0}: ", errorEntry.Key);

                            foreach (var affectedItem in affectedItems)
                            {
                                AppendLine(mailBody, "  {0}", affectedItem.IssueDetail);

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
                                AppendLine(mailBody, "  {0}", infoItem);
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
                    }

                    if (instrumentFilePaths.Count == 1)
                    {
                        AppendLine(mailBody, "Instrument file:{0}{1}", Environment.NewLine, instrumentFilePaths.First());
                    }
                    else if (instrumentFilePaths.Count > 1)
                    {
                        mailBody.AppendLine("Instrument files:");

                        foreach (var triggerFile in instrumentFilePaths)
                        {
                            AppendLine(mailBody, "  {0}", triggerFile);
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
                        foreach (var subject in (from item in subjectList where item.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0 select item))
                        {
                            mailToSend.Subject = subject;
                            break;
                        }
                    }

                    mailBody.AppendLine();
                    mailBody.AppendLine("Log file location:");
                    AppendLine(mailBody, "  {0}", GetLogFileSharePath());
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
                        AppendLine(mailContentPreview, "To: {0}", recipients);
                        AppendLine(mailContentPreview, "Subject: {0}", mailToSend.Subject);
                        mailContentPreview.AppendLine();
                        mailContentPreview.AppendLine(mailToSend.Body);
                        mailContentPreview.AppendLine();
                    }
                    else
                    {
                        currentTask = "Send the mail";
                        var smtp = new SmtpClient(mailServer);
                        smtp.Send(mailToSend);

                        AppUtils.SleepMilliseconds(100);
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
                }

                currentTask = "Preview cached messages";

                if (MailDisabled && mailContentPreview.Length > 0)
                {
                    ShowTraceMessage("Mail content preview" + Environment.NewLine + mailContentPreview);
                }
            }
            catch (Exception ex)
            {
                var msg = "Error in SendQueuedMail, task " + currentTask;
                LogErrorToDatabase(msg, ex);
                throw new Exception(msg, ex);
            }
        }

        /// <summary>
        /// Show a message at the console, preceded by a time stamp
        /// </summary>
        /// <param name="message">Trace message</param>
        private void ShowTrace(string message)
        {
            if (!TraceMode)
                return;

            ShowTraceMessage(message);
        }

        /// <summary>
        /// Show a message at the console, preceded with a timestamp
        /// </summary>
        /// <param name="message">Trace message</param>
        public static void ShowTraceMessage(string message)
        {
            BaseLogger.ShowTraceMessage(message, false);
        }
    }
}
