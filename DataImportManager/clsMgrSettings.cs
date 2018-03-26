using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using PRISM;

namespace DataImportManager
{
    /// <summary>
    /// Class for loading, storing and accessing manager parameters.
    /// </summary>
    /// <remarks>
    ///   Loads initial settings from local config file, then checks to see if remainder of settings should be
    ///   loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
    ///   parameters database.
    /// </remarks>
    // ReSharper disable once InconsistentNaming
    public class clsMgrSettings : clsLoggerBase
    {
        #region "Constants"

        /// <summary>
        /// Status message for when the manager is deactivated locally
        /// </summary>
        /// <remarks>Used when MgrActive_Local is False in AppName.exe.config</remarks>
        public const string DEACTIVATED_LOCALLY = "Manager deactivated locally";

        /// <summary>
        /// Status message for when the settings could not be loaded
        /// This includes if the manager name is not defined in the manager control database
        /// </summary>
        public const string ERROR_INITIALIZING_MANAGER_SETTINGS = "Unable to initialize manager settings class";

        /// <summary>
        /// Manager parameter: config database connection string
        /// </summary>
        public const string MGR_PARAM_MGR_CFG_DB_CONN_STRING = "MgrCnfgDbConnectStr";

        /// <summary>
        /// Manager parameter: manager active
        /// </summary>
        /// <remarks>Defined in AppName.exe.config</remarks>
        public const string MGR_PARAM_MGR_ACTIVE_LOCAL = "MgrActive_Local";

        /// <summary>
        /// Manager parameter: manager name
        /// </summary>
        public const string MGR_PARAM_MGR_NAME = "MgrName";

        /// <summary>
        /// Manager parameter: using defaults flag
        /// </summary>
        public const string MGR_PARAM_USING_DEFAULTS = "UsingDefaults";

        #endregion

        #region "Class variables"

        private readonly Dictionary<string, string> mParamDictionary;

        // ReSharper disable once InconsistentNaming
        private bool mMCParamsLoaded;

        private string mErrMsg = string.Empty;

        #endregion

        #region "Properties"

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrMsg => mErrMsg;

        /// <summary>
        /// True when the manager is deactivated, either locally or in the database
        /// </summary>
        public bool ManagerDeactivated { get; private set; }

        /// <summary>
        /// Manager name
        /// </summary>
        public string ManagerName => GetParam(MGR_PARAM_MGR_NAME, Environment.MachineName + "_Undefined-Manager");

        /// <summary>
        /// Manager parameters dictionary
        /// </summary>
        public Dictionary<string, string> TaskDictionary => mParamDictionary;

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="traceMode"></param>
        public clsMgrSettings(bool traceMode)
        {
            TraceMode = traceMode;

            mParamDictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var success = LoadSettings();

            if (TraceMode)
            {
                ShowTraceMessage("Initialized clsMgrSettings");
                ShowDictionaryTrace(mParamDictionary);
            }

            if (success) return;

            if (string.Equals(ErrMsg, DEACTIVATED_LOCALLY))
                throw new ApplicationException(DEACTIVATED_LOCALLY);

            throw new ApplicationException(ERROR_INITIALIZING_MANAGER_SETTINGS + ": " + ErrMsg);

        }

        /// <summary>
        /// Disable the manager by changing MgrActive_Local to False in DataImportManager.exe.config
        /// </summary>
        /// <returns></returns>
        public bool DisableManagerLocally()
        {
            return WriteConfigSetting(MGR_PARAM_MGR_ACTIVE_LOCAL, "False");
        }

        /// <summary>
        /// Updates manager settings, then loads settings from the database or from ManagerSettingsLocal.xml if clsUtilities.OfflineMode is true
        /// </summary>
        /// <returns>True if successful; False on error</returns>
        /// <remarks></remarks>
        public bool LoadSettings()
        {
            // Get settings from config file
            var configFileSettings = LoadMgrSettingsFromFile();

            return LoadSettings(configFileSettings);
        }

        /// <summary>
        /// Updates manager settings, then loads settings from the database
        /// </summary>
        /// <param name="configFileSettings">Manager settings loaded from file AppName.exe.config</param>
        /// <returns>True if successful; False on error</returns>
        /// <remarks></remarks>
        public bool LoadSettings(Dictionary<string, string> configFileSettings)
        {
            mErrMsg = string.Empty;

            mParamDictionary.Clear();

            foreach (var item in configFileSettings)
            {
                mParamDictionary.Add(item.Key, item.Value);
            }

            // Get directory for main executable
            var exeDirectoryPath = clsGlobal.GetExeDirectoryPath();
            mParamDictionary.Add("ApplicationPath", exeDirectoryPath);

            // Test the settings retrieved from the config file
            if (!CheckInitialSettings(mParamDictionary))
            {
                // Error logging handled by CheckInitialSettings
                return false;
            }

            // Determine if manager is deactivated locally
            if (!mParamDictionary.TryGetValue(MGR_PARAM_MGR_ACTIVE_LOCAL, out var activeLocalText))
            {
                mErrMsg = "Manager parameter " + MGR_PARAM_MGR_ACTIVE_LOCAL + " is missing from file " + Path.GetFileName(GetConfigFilePath());
                LogError(mErrMsg);
            }

            if (!bool.TryParse(activeLocalText, out var activeLocal) || !activeLocal)
            {
                LogWarning(DEACTIVATED_LOCALLY);
                mErrMsg = DEACTIVATED_LOCALLY;
                ManagerDeactivated = true;
                return false;
            }

            // Get remaining settings from database
            if (!LoadMgrSettingsFromDatabase())
            {
                // Error logging handled by LoadMgrSettingsFromDatabase
                return false;
            }

            var mgrActive = GetParam("mgractive", true);
            ManagerDeactivated = !mgrActive;

            // Set flag indicating manager parameters have been loaded
            mMCParamsLoaded = true;

            // No problems found
            return true;
        }

        private Dictionary<string, string> LoadMgrSettingsFromFile()
        {
            // Note: When you are editing this project using the Visual Studio IDE, if you edit the values
            // ->Properties>Settings.settings, when you run the program (from within the IDE), it
            // will update file CaptureTaskManager.exe.config with your settings

            // Load initial settings into string dictionary for return
            var mgrSettingsFromFile = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Manager config DB connection string
            var mgrCfgDbConnString = Properties.Settings.Default.MgrCnfgDbConnectStr;
            mgrSettingsFromFile.Add(MGR_PARAM_MGR_CFG_DB_CONN_STRING, mgrCfgDbConnString);

            // Manager active flag
            var mgrActiveLocal = Properties.Settings.Default.MgrActive_Local.ToString();
            mgrSettingsFromFile.Add(MGR_PARAM_MGR_ACTIVE_LOCAL, mgrActiveLocal);

            // Manager name
            // If the MgrName setting in the AppName.exe.config file contains the text $ComputerName$
            // that text is replaced with this computer's domain name
            // This is a case-sensitive comparison
            //
            var managerName = Properties.Settings.Default.MgrName;
            var autoDefinedName = managerName.Replace("$ComputerName$", Environment.MachineName);

            if (!string.Equals(managerName, autoDefinedName))
            {
                ShowTraceMessage("Auto-defining the manager name as " + autoDefinedName);
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, autoDefinedName);
            }
            else
            {
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, managerName);
            }

            // Default settings in use flag
            var usingDefaults = Properties.Settings.Default.UsingDefaults.ToString();
            mgrSettingsFromFile.Add(MGR_PARAM_USING_DEFAULTS, usingDefaults);

            if (TraceMode)
            {
                var configFilePath = clsGlobal.GetExePath() + ".config";
                ShowTraceMessage("Settings loaded from " + clsPathUtils.CompactPathString(configFilePath, 80));
                ShowDictionaryTrace(mgrSettingsFromFile);
            }

            return mgrSettingsFromFile;
        }

        /// <summary>
        /// Tests initial settings retrieved from config file
        /// </summary>
        /// <param name="paramDictionary"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool CheckInitialSettings(IReadOnlyDictionary<string, string> paramDictionary)
        {
            // Verify manager settings dictionary exists
            if (paramDictionary == null)
            {
                mErrMsg = "CheckInitialSettings: Manager parameter string dictionary not found";
                LogError(mErrMsg, true);
                return false;
            }

            // Verify intact config file was found
            if (!paramDictionary.TryGetValue(MGR_PARAM_USING_DEFAULTS, out var usingDefaultsText))
            {
                mErrMsg = "CheckInitialSettings: 'UsingDefaults' entry not found in Config file";
                LogError(mErrMsg, true);
            }
            else
            {
                if (bool.TryParse(usingDefaultsText, out var usingDefaults) && usingDefaults)
                {
                    mErrMsg = "CheckInitialSettings: Config file problem, contains UsingDefaults=True";
                    LogError(mErrMsg, true);
                    return false;
                }
            }

            // No problems found
            return true;
        }


        /// <summary>
        /// Gets manager config settings from manager control DB (Manager_Control)
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>Performs retries if necessary.</remarks>
        public bool LoadMgrSettingsFromDatabase(bool logConnectionErrors = true)
        {

            var managerName = GetParam(MGR_PARAM_MGR_NAME, string.Empty);

            if (string.IsNullOrEmpty(managerName))
            {
                mErrMsg = "Manager parameter " + MGR_PARAM_MGR_NAME + " is missing from file " + Path.GetFileName(GetConfigFilePath());
                LogError(mErrMsg);
                return false;
            }

            var success = LoadMgrSettingsFromDatabase(managerName, out var mgrParameters, logConnectionErrors, returnErrorIfNoParameters: true);
            if (!success)
            {
                return false;
            }

            success = StoreParameters(mgrParameters, skipExistingParameters: false, managerName: managerName);

            return success;
        }

        private bool LoadMgrSettingsFromDatabase(
            string managerName,
            out Dictionary<string, string> mgrParameters,
            bool logConnectionErrors,
            bool returnErrorIfNoParameters)
        {
            const short retryCount = 6;

            mgrParameters = new Dictionary<string, string>();

            // Data Source=proteinseqs;Initial Catalog=manager_control
            var connectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING, string.Empty);

            if (string.IsNullOrEmpty(managerName))
            {
                mErrMsg = "MgrCnfgDbConnectStr parameter not found in m_ParamDictionary; " +
                          "it should be defined in the " + Path.GetFileName(GetConfigFilePath()) + " file";

                if (TraceMode)
                    ShowTraceMessage("LoadMgrSettingsFromDatabase: " + mErrMsg);

                return false;
            }

            if (string.IsNullOrEmpty(connectionString))
            {
                mErrMsg = MGR_PARAM_MGR_CFG_DB_CONN_STRING +
                           " parameter not found in mParamDictionary; it should be defined in the " + Path.GetFileName(GetConfigFilePath()) + " file";
                WriteErrorMsg(mErrMsg);
                return false;
            }

            if (TraceMode)
                ShowTraceMessage("LoadMgrSettingsFromDatabase using [" + connectionString + "] for manager " + managerName);

            var sqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + managerName + "'";

            var dbTools = new clsDBTools(connectionString);

            // Run the query
            var success = dbTools.GetQueryResults(sqlStr, out var lstResults, "LoadMgrSettingsFromDatabase", retryCount);

            // If unable to retrieve the data, return false
            if (!success)
            {
                // Log the message to the DB if the monthly Windows updates are not pending
                var allowLogToDb = !clsWindowsUpdateStatus.ServerUpdatesArePending();

                mErrMsg = "LoadMgrSettingsFromDatabase; Excessive failures attempting to retrieve manager settings from database " +
                          "for manager '" + managerName + "'";
                if (logConnectionErrors)
                    WriteErrorMsg(mErrMsg, allowLogToDb);
                return false;
            }

            // Verify at least one row returned
            if (lstResults.Count < 1 && returnErrorIfNoParameters)
            {
                // No data was returned
                mErrMsg = "LoadMgrSettingsFromDatabase; Manager '" + managerName + "' not defined in the manager control database; using " + connectionString;
                if (logConnectionErrors)
                    WriteErrorMsg(mErrMsg);
                return false;
            }

            foreach (var resultRow in lstResults)
            {
                if (resultRow.Count < 2)
                    continue;

                var paramName = resultRow[0];
                var paramValue = resultRow[1];

                mgrParameters.Add(paramName, paramValue);
            }

            return true;
        }

        /// <summary>
        /// Update mParamDictionary with settings in lstSettings, optionally skipping existing parameters
        /// </summary>
        /// <param name="mgrParameters"></param>
        /// <param name="skipExistingParameters"></param>
        /// <param name="managerName"></param>
        /// <returns></returns>
        private bool StoreParameters(IReadOnlyDictionary<string, string> mgrParameters, bool skipExistingParameters, string managerName)
        {
            bool success;

            try
            {
                foreach (var mgrParam in mgrParameters)
                {
                    // Add the column heading and value to the dictionary
                    var paramKey = mgrParam.Key;
                    var paramVal = mgrParam.Value;

                    if (paramKey.ToLower() == "perspective" && Environment.MachineName.ToLower().StartsWith("monroe"))
                    {
                        if (paramVal.ToLower() == "server")
                        {
                            paramVal = "client";
                            Console.WriteLine(
                                @"StoreParameters: Overriding manager perspective to be 'client' because impersonating a server-based manager from an office computer");
                        }
                    }

                    if (mParamDictionary.ContainsKey(paramKey))
                    {
                        if (!skipExistingParameters)
                        {
                            mParamDictionary[paramKey] = paramVal;
                        }
                    }
                    else
                    {
                        mParamDictionary.Add(paramKey, paramVal);
                    }
                }
                success = true;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsAnalysisMgrSettings.StoreParameters; Exception filling string dictionary from table for manager " +
                          "'" + managerName + "': " + ex.Message;
                WriteErrorMsg(mErrMsg);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Gets a parameter from the parameters string dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <returns>String value associated with specified key</returns>
        /// <remarks>Returns Nothing if key isn't found</remarks>
        public string GetParam(string itemKey)
        {
            if (mParamDictionary == null)
                return string.Empty;

            if (!mParamDictionary.TryGetValue(itemKey, out var value))
                return string.Empty;

            return string.IsNullOrWhiteSpace(value) ? string.Empty : value;
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public bool GetParam(string itemKey, bool valueIfMissing)
        {
            if (bool.TryParse(GetParam(itemKey), out var boolValue))
                return boolValue;

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public string GetParam(string itemKey, string valueIfMissing)
        {
            var value = GetParam(itemKey);
            if (string.IsNullOrEmpty(value))
            {
                return valueIfMissing;
            }

            return value;
        }

        /// <summary>
        /// Sets a parameter in the parameters string dictionary
        /// </summary>
        /// <param name="itemKey">Key name for the item</param>
        /// <param name="itemValue">Value to assign to the key</param>
        /// <remarks></remarks>
        public void SetParam(string itemKey, string itemValue)
        {
            if (mParamDictionary.ContainsKey(itemKey))
            {
                mParamDictionary[itemKey] = itemValue;
            }
            else
            {
                mParamDictionary.Add(itemKey, itemValue);
            }
        }

        /// <summary>
        /// Show contents of a dictionary
        /// </summary>
        /// <param name="settings"></param>
        public static void ShowDictionaryTrace(IReadOnlyDictionary<string, string> settings)
        {
            Console.ForegroundColor = ConsoleMsgUtils.DebugFontColor;
            foreach (var key in from item in settings.Keys orderby item select item)
            {
                var value = settings[key];
                var keyWidth = Math.Max(30, Math.Ceiling(key.Length / 15.0) * 15);
                var formatString = "  {0,-" + keyWidth + "} {1}";
                Console.WriteLine(formatString, key, value);
            }
            Console.ResetColor();
        }

        private static void ShowTraceMessage(string message)
        {
            clsMainProcess.ShowTraceMessage(message);
        }


        /// <summary>
        /// Writes specfied value to an application config file.
        /// </summary>
        /// <param name="key">Name for parameter (case sensitive)</param>
        /// <param name="value">New value for parameter</param>
        /// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
        /// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
        public bool WriteConfigSetting(string key, string value)
        {
            mErrMsg = string.Empty;

            // Load the config document
            var myDoc = LoadConfigDocument();
            if (myDoc == null)
            {
                // Error message has already been produced by LoadConfigDocument
                return false;
            }

            // Retrieve the settings node
            var myNode = myDoc.SelectSingleNode("// applicationSettings");

            if (myNode == null)
            {
                mErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; applicationSettings node not found";
                return false;
            }

            try
            {
                // Select the element containing the value for the specified key containing the key
                var myElement = (XmlElement)myNode.SelectSingleNode(string.Format("// setting[@name='{0}']/value", key));
                if (myElement != null)
                {
                    // Set key to specified value
                    myElement.InnerText = value;
                }
                else
                {
                    // Key was not found
                    mErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                myDoc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Loads an app config file for changing parameters
        /// </summary>
        /// <returns>App config file as an XML document if successful; NOTHING on failure</returns>
        /// <remarks></remarks>
        private XmlDocument LoadConfigDocument()
        {
            try
            {
                var doc = new XmlDocument();
                doc.Load(GetConfigFilePath());
                return doc;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsAnalysisMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
                return null;
            }
        }

        /// <summary>
        /// Writes an error message to the application log and the database
        /// </summary>
        /// <param name="errorMessage">Message to write</param>
        /// <param name="allowLogToDb"></param>
        /// <remarks></remarks>
        private void WriteErrorMsg(string errorMessage, bool allowLogToDb = true)
        {
            var logToDb = !mMCParamsLoaded && allowLogToDb;
            LogError(errorMessage, logToDb);

            if (TraceMode)
            {
                ShowTraceMessage(errorMessage);
            }
        }

        /// <summary>
        /// Specifies the full name and path for the application config file
        /// </summary>
        /// <returns>String containing full name and path</returns>
        private string GetConfigFilePath()
        {
            var configFilePath = clsGlobal.GetExePath() + ".config";
            return configFilePath;
        }

    }
}
