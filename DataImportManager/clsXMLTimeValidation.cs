using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class clsXMLTimeValidation : clsLoggerBase
    {

        #region "Member variables"

        private string mDatasetName = string.Empty;

        private DateTime mRunFinishUtc = new DateTime(1960, 1, 1);

        private string mCaptureType = string.Empty;

        private string mSourcePath = string.Empty;

        /// <summary>
        /// Operator username, aka OperatorPRN
        /// </summary>
        private string mOperatorUsername = string.Empty;

        private string mOperatorEmail = string.Empty;

        private string mOperatorName = string.Empty;

        private string mDatasetPath = string.Empty;
        private ShareConnector mShareConnector;

        private int mSleepInterval = 30;

        // ReSharper disable once InconsistentNaming
        private readonly DMSInfoCache mDMSInfoCache;

        private readonly clsProcessXmlTriggerFile.XmlProcSettingsType mProcSettings;

        private readonly ConcurrentDictionary<string, int> mInstrumentsToSkip;

        private readonly FileTools mFileTools;

        private readonly clsMgrSettings mMgrParams;

        #endregion

        #region "Properties"

        public string CaptureSubfolder { get; private set; } = string.Empty;

        public string DatasetName => FixNull(mDatasetName);

        /// <summary>
        /// Source path to the dataset on the instrument
        /// </summary>
        public string DatasetPath => FixNull(mDatasetPath);

        public string InstrumentName { get; private set; } = string.Empty;

        public string ErrorMessage { get; private set; } = string.Empty;

        public string OperatorEMail => FixNull(mOperatorEmail);

        public string OperatorName => FixNull(mOperatorName);

        // ReSharper disable once InconsistentNaming
        public string OperatorPRN => FixNull(mOperatorUsername);

        /// <summary>
        /// Source path on the instrument, e.g. \\TSQ_1\ProteomicsData
        /// </summary>
        public string SourcePath => FixNull(mSourcePath);

        public bool TraceMode { get; set; }

        #endregion

        #region "Enums"

        private enum RawDsTypes
        {
            None = 0,
            File = 1,
            FolderNoExt = 2,
            FolderExt = 3
        }

        public enum XmlValidateStatus
        {
            // ReSharper disable InconsistentNaming
            XML_VALIDATE_SUCCESS = 0,
            XML_VALIDATE_FAILED = 1,
            [Obsolete("Old enum")]
            XML_VALIDATE_NO_CHECK = 2,
            XML_VALIDATE_ENCOUNTERED_ERROR = 3,
            XML_VALIDATE_BAD_XML = 4,
            XML_VALIDATE_CONTINUE = 5,
            XML_WAIT_FOR_FILES = 6,
            XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE = 7,
            XML_VALIDATE_SIZE_CHANGED = 8,
            XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR = 9,
            XML_VALIDATE_NO_DATA = 10,
            XML_VALIDATE_SKIP_INSTRUMENT = 11,
            XML_VALIDATE_NO_OPERATOR = 12,
            XML_VALIDATE_TRIGGER_FILE_MISSING = 13,
            // ReSharper restore InconsistentNaming
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="dctInstrumentsToSkip"></param>
        /// <param name="dmsCache"></param>
        /// <param name="udtProcSettings"></param>
        /// <remarks></remarks>
        public clsXMLTimeValidation(
            clsMgrSettings mgrParams,
            ConcurrentDictionary<string, int> dctInstrumentsToSkip,
            DMSInfoCache dmsCache,
            clsProcessXmlTriggerFile.XmlProcSettingsType udtProcSettings)
        {
            mMgrParams = mgrParams;
            mFileTools = new FileTools();
            mInstrumentsToSkip = dctInstrumentsToSkip;
            mDMSInfoCache = dmsCache;
            mProcSettings = udtProcSettings;
        }

        private string FixNull(string strText)
        {
            if (string.IsNullOrEmpty(strText))
            {
                return string.Empty;
            }

            return strText;
        }

        public XmlValidateStatus ValidateXmlFile(FileInfo triggerFile)
        {
            XmlValidateStatus rslt;
            ErrorMessage = string.Empty;
            try
            {
                if (TraceMode)
                {
                    ShowTraceMessage("Reading " + triggerFile.FullName);
                }

                rslt = GetXmlParameters(triggerFile);
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error reading the XML file " + triggerFile.Name;
                var errMsg = "clsXMLTimeValidation.ValidateXMLFile(), " + ErrorMessage + ": " + ex.Message;
                LogError(errMsg);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }

            if (rslt != XmlValidateStatus.XML_VALIDATE_CONTINUE)
            {
                return rslt;
            }

            if (mInstrumentsToSkip.ContainsKey(InstrumentName))
            {
                return XmlValidateStatus.XML_VALIDATE_SKIP_INSTRUMENT;
            }

            try
            {
                rslt = SetDbInstrumentParameters(InstrumentName);
                if (rslt == XmlValidateStatus.XML_VALIDATE_CONTINUE)
                {
                    rslt = PerformValidation();
                }
                else
                {
                    return rslt;
                }

            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception calling PerformValidation";
                LogError("clsXMLTimeValidation.ValidateXMLFile(), Error calling PerformValidation", ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }

            return rslt;
        }

        // Take the xml file and load into a dataset
        // iterate through the dataset to retrieve the instrument name
        private XmlValidateStatus GetXmlParameters(FileInfo triggerFile)
        {
            // initialize return value
            var rslt = XmlValidateStatus.XML_VALIDATE_CONTINUE;

            var xmlFileContents = clsGlobal.LoadXmlFileContentsIntoString(triggerFile);
            if (string.IsNullOrEmpty(xmlFileContents))
            {
                return XmlValidateStatus.XML_VALIDATE_TRIGGER_FILE_MISSING;
            }

            // Load into a string reader after '&' was fixed
            TextReader xmlStringReader = new StringReader(xmlFileContents);
            try
            {
                var xmlDataSet = new DataSet();
                xmlDataSet.ReadXml(xmlStringReader);

                // Everything must be OK if we got to here
                foreach (DataTable table in xmlDataSet.Tables)
                {
                    foreach (DataRow row in table.Rows)
                    {
                        var parameterName = row["Name"].ToString();
                        switch (parameterName)
                        {
                            case "Instrument Name":
                                InstrumentName = row["Value"].ToString();
                                break;

                            case "Capture Subfolder":
                                CaptureSubfolder = row["Value"].ToString();
                                if (Path.IsPathRooted(CaptureSubfolder))
                                {
                                    // Instrument folder has an older version of Buzzard that incorrectly determines the capture subfolder
                                    // For safety, will blank this out, but will post a log entry to the database
                                    var msg = "clsXMLTimeValidation.GetXMLParameters(), the CaptureSubfolder is not a relative path; " +
                                        "this indicates a bug with Buzzard; see: " + triggerFile.Name;

                                    LogError(msg, null, true);
                                    CaptureSubfolder = string.Empty;
                                }
                                break;

                            case "Dataset Name":
                                mDatasetName = row["Value"].ToString();
                                break;

                            case "Run Finish UTC":
                                mRunFinishUtc = DateTime.Parse(row["Value"].ToString());
                                break;

                            case "Operator (PRN)":
                                mOperatorUsername = row["Value"].ToString();
                                break;
                        }
                    }

                }

                if (string.IsNullOrEmpty(InstrumentName))
                {
                    LogError("clsXMLTimeValidation.GetXMLParameters(), The instrument name was blank.");
                    return XmlValidateStatus.XML_VALIDATE_BAD_XML;
                }

                if (InstrumentName.StartsWith("9T") ||
                    InstrumentName.StartsWith("11T") ||
                    InstrumentName.StartsWith("12T"))
                {
                    rslt = InstrumentWaitDelay(triggerFile);
                }

                if (rslt != XmlValidateStatus.XML_VALIDATE_CONTINUE)
                {
                    return rslt;
                }

                return XmlValidateStatus.XML_VALIDATE_CONTINUE;

            }
            catch (Exception ex)
            {
                LogError("clsXMLTimeValidation.GetXMLParameters(), Error reading XML File", ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }

        }

        private XmlValidateStatus InstrumentWaitDelay(FileSystemInfo triggerFile)
        {
            try
            {
                var delayValue = int.Parse(mMgrParams.GetParam("xmlfiledelay"));
                var fileModDate = triggerFile.LastWriteTimeUtc;
                var fileModDateDelay = fileModDate.AddMinutes(delayValue);
                var dateNow = DateTime.UtcNow;

                if (dateNow >= fileModDateDelay)
                    return XmlValidateStatus.XML_VALIDATE_CONTINUE;

                LogWarning("clsXMLTimeValidation.InstrumentWaitDelay(), The dataset import is being delayed for XML File: " + triggerFile.Name);
                return XmlValidateStatus.XML_WAIT_FOR_FILES;

            }
            catch (Exception ex)
            {
                LogError("clsXMLTimeValidation.InstrumentWaitDelay(), Error determining wait delay", ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }

        }

        /// <summary>
        /// Query to get the instrument data from the database and then iterate through the dataset to retrieve the capture type and source path
        /// </summary>
        /// <param name="insName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private XmlValidateStatus SetDbInstrumentParameters(string insName)
        {
            try
            {
                // Requests additional task parameters from database and adds them to the m_taskParams string dictionary
                if (!mDMSInfoCache.GetInstrumentInfo(insName, out var udtInstrumentInfo))
                {
                    LogError(
                        "clsXMLTimeValidation.SetDbInstrumentParameters(), Instrument " +
                        insName + " not found in data from V_Instrument_List_Export");
                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
                }

                mCaptureType = udtInstrumentInfo.CaptureType;
                mSourcePath = udtInstrumentInfo.SourcePath;

                if (string.IsNullOrWhiteSpace(mCaptureType))
                {
                    clsMainProcess.LogErrorToDatabase(
                        "clsXMLTimeValidation.SetDbInstrumentParameters(), Instrument " +
                        insName + " has an empty value for Capture in V_Instrument_List_Export");

                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
                }

                if (!string.IsNullOrWhiteSpace(mSourcePath))
                    return XmlValidateStatus.XML_VALIDATE_CONTINUE;

                clsMainProcess.LogErrorToDatabase(
                    "clsXMLTimeValidation.SetDbInstrumentParameters(), Instrument " + insName +
                    " has an empty value for SourcePath in V_Instrument_List_Export");

                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;

            }
            catch (Exception ex)
            {
                LogError(
                    "clsXMLTimeValidation.SetDbInstrumentParameters(), " +
                    "Error retrieving source path and capture type for instrument: " + insName, ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }

        }

        private XmlValidateStatus PerformValidation()
        {
            var connected = false;
            var currentTask = string.Empty;
            var ignoreInstrumentSourceErrors = mProcSettings.IgnoreInstrumentSourceErrors;

            try
            {
                if (string.IsNullOrEmpty(mCaptureType) || string.IsNullOrEmpty(mSourcePath))
                {
                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
                }

                if (string.Equals(mCaptureType, "secfso", StringComparison.OrdinalIgnoreCase))
                {
                    // Make sure mSourcePath is not of the form \\proto-2 because if that is the case, then mCaptureType should be "fso"
                    var reProtoServer = new Regex(@"\\\\proto-\d+\\", (RegexOptions.Compiled | RegexOptions.IgnoreCase));

                    if (reProtoServer.IsMatch(mSourcePath))
                    {
                        // Auto-change mCaptureType to "fso", and log an error in the database
                        mCaptureType = "fso";
                        var errMsg = "Instrument " + InstrumentName + " is configured to use 'secfso' yet its source folder is " +
                            mSourcePath + ", which appears to be a domain path; auto-changing the capture_method to 'fso' for now, " +
                            "but the configuration in the database should be updated (see table T_Instrument_Name)";

                        if (TraceMode)
                        {
                            Console.WriteLine(" - - - - - - - - ");
                            ShowTraceMessage("ERROR: " + errMsg);
                            Console.WriteLine(" - - - - - - - - ");
                        }

                        clsMainProcess.LogErrorToDatabase(errMsg);
                    }

                }

                // Define the source path now, before attempting to connect to Bionet
                // This is done so that mDatasetPath will be defined so we can include it in a log message if a connection error occurs
                string datasetSourcePath;

                if (string.IsNullOrWhiteSpace(CaptureSubfolder))
                {
                    datasetSourcePath = string.Copy(mSourcePath);
                }
                else
                {
                    datasetSourcePath = Path.Combine(mSourcePath, CaptureSubfolder);
                }

                // Initially define this as the dataset source folder and the dataset name
                // It will later be updated to have the actual instrument file or folder name
                mDatasetPath = Path.Combine(datasetSourcePath, mDatasetName);

                if (string.Equals(mCaptureType, "secfso", StringComparison.OrdinalIgnoreCase) &&
                    !clsGlobal.GetHostName().ToLower().StartsWith("monroe"))
                {
                    // Source folder is on bionet; establish a connection
                    var username = mMgrParams.GetParam("bionetuser");
                    var encodedPwd = mMgrParams.GetParam("bionetpwd");
                    if (!username.Contains('\\'))
                    {
                        // Prepend this computer's name to the username
                        username = clsGlobal.GetHostName() + '\\' + username;
                    }

                    var currentTaskBase = "Connecting to " + mSourcePath + " using secfso, user " + username + "," + " and encoded password " + encodedPwd;
                    currentTask = currentTaskBase + "; Decoding password";
                    if (TraceMode)
                    {
                        ShowTraceMessage(currentTask);
                    }

                    var decodedPwd = Pacifica.Core.Utilities.DecodePassword(encodedPwd);
                    currentTask = currentTaskBase + "; Instantiating ShareConnector";
                    if (TraceMode)
                    {
                        ShowTraceMessage(currentTask);
                    }

                    mShareConnector = new ShareConnector(username, decodedPwd)
                    {
                        Share = mSourcePath
                    };

                    currentTask = currentTaskBase + "; Connecting using ShareConnector";
                    if (TraceMode)
                    {
                        ShowTraceMessage(currentTask);
                    }

                    if (mShareConnector.Connect())
                    {
                        connected = true;
                    }
                    else
                    {
                        currentTask = currentTaskBase + "; Error connecting";
                        ErrorMessage = "Error "
                                    + mShareConnector.ErrorMessage + " connecting to "
                                    + mSourcePath + " as user "
                                    + username + " using 'secfso'" + "; error code " + mShareConnector.ErrorMessage;

                        LogError(ErrorMessage);

                        switch (mShareConnector.ErrorMessage)
                        {
                            case "1326":
                                LogError("You likely need to change the Capture_Method from secfso to fso; use the following query: ");
                                break;

                            case "53":
                                LogError("The password may need to be reset; diagnose things further using the following query: ");
                                break;

                            case "1219":
                            case "1203":
                                var statusMsg = "Likely had error 'An unexpected network error occurred' while validating the Dataset " +
                                                "specified by the XML file (ErrorMessage=" + mShareConnector.ErrorMessage + ")";
                                if (TraceMode)
                                {
                                    ShowTraceMessage(statusMsg);
                                }

                                // Likely had error "An unexpected network error occurred" while validating the Dataset specified by the XML file
                                // Need to completely exit the manager
                                if (mProcSettings.IgnoreInstrumentSourceErrors)
                                {
                                    ignoreInstrumentSourceErrors = true;
                                }
                                else
                                {
                                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR;
                                }
                                break;

                            default:
                                LogError("You can diagnose the problem using this query: ");
                                break;
                        }

                        if (!ignoreInstrumentSourceErrors)
                        {
                            if (mProcSettings.IgnoreInstrumentSourceErrors)
                            {
                                ignoreInstrumentSourceErrors = true;
                            }
                            else
                            {
                                var sqlQuery =
                                    "SELECT Inst.IN_name, SP.SP_path_ID, SP.SP_path, SP.SP_machine_name, SP.SP_vol_name_client, " +
                                    "       SP.SP_vol_name_server, SP.SP_function, Inst.IN_capture_method " +
                                    "FROM T_Storage_Path SP INNER JOIN T_Instrument_Name Inst " +
                                    "       ON SP.SP_instrument_name = Inst.IN_name AND SP.SP_path_ID = Inst.IN_source_path_ID " +
                                    "WHERE IN_Name = '" + InstrumentName + "'";

                                LogError(sqlQuery);

                                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
                            }

                        }

                    }

                }

                // Make sure mSleepInterval isn't too large
                if (mSleepInterval > 900)
                {
                    LogWarning("Sleep interval of " + mSleepInterval + " seconds is too large; decreasing to 900 seconds");
                    mSleepInterval = 900;
                }

                // Determine dataset type
                currentTask = "Determining dataset type for " + mDatasetName + " at " + datasetSourcePath;
                if (TraceMode)
                {
                    ShowTraceMessage(currentTask);
                }

                var resType = GetRawDsType(mSourcePath, CaptureSubfolder, mDatasetName, ignoreInstrumentSourceErrors, out var instrumentFileOrFolderName);

                currentTask = "Validating operator name " + mOperatorUsername + " for " + mDatasetName + " at " + datasetSourcePath;
                if (TraceMode)
                {
                    ShowTraceMessage(currentTask);
                }

                if (!SetOperatorName())
                {
                    if (connected)
                    {
                        currentTask = "Operator not found; disconnecting from " + mSourcePath;
                        if (TraceMode)
                        {
                            ShowTraceMessage(currentTask);
                        }

                        DisconnectShare(mShareConnector);
                    }

                    return XmlValidateStatus.XML_VALIDATE_NO_OPERATOR;
                }

                switch (resType)
                {
                    case RawDsTypes.None:
                        // No raw dataset file or folder found
                        currentTask = "Dataset not found at " + datasetSourcePath;
                        if (TraceMode)
                        {
                            ShowTraceMessage(currentTask);
                        }

                        // Disconnect from BioNet if necessary
                        if (connected)
                        {
                            currentTask = "Dataset not found; disconnecting from " + mSourcePath;
                            if (TraceMode)
                            {
                                ShowTraceMessage(currentTask);
                            }

                            DisconnectShare(mShareConnector);
                        }

                        return XmlValidateStatus.XML_VALIDATE_NO_DATA;

                    case RawDsTypes.File:
                        // Dataset file found
                        // Check the file size
                        currentTask = "Dataset found at " + datasetSourcePath + "; verifying file size is constant";
                        if (TraceMode)
                        {
                            ShowTraceMessage(currentTask);
                        }

                        // Update the dataset path to include the instrument file or folder name
                        mDatasetPath = Path.Combine(datasetSourcePath, instrumentFileOrFolderName);

                        if (ignoreInstrumentSourceErrors && !File.Exists(mDatasetPath))
                        {
                            // Assume the file is a constant size
                            LogWarning("File not found, but assuming constant size: " + mDatasetPath);
                        }
                        else if (!VerifyConstantFileSize(mDatasetPath, mSleepInterval, out var logonFailure))
                        {
                            if (!logonFailure)
                            {
                                LogWarning("Dataset '" + mDatasetName + "' not ready (file size changed over " + mSleepInterval + " seconds)");
                            }

                            if (connected)
                            {
                                currentTask = "Dataset size changed; disconnecting from " + mSourcePath;
                                if (TraceMode)
                                {
                                    ShowTraceMessage(currentTask);
                                }

                                DisconnectShare(mShareConnector);
                            }

                            return logonFailure ? XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE : XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED;

                        }

                        currentTask = "Dataset found at " + datasetSourcePath + " and is unchanged";
                        if (TraceMode)
                        {
                            ShowTraceMessage(currentTask);
                        }

                        if (clsGlobal.GetHostName().ToLower().StartsWith("monroe"))
                        {
                            Console.WriteLine("Skipping date validation because host name starts with Monroe");
                        }
                        else if (mRunFinishUtc != new DateTime(1960, 1, 1))
                        {
                            var taskAddon = "validating file Date vs. Run_Finish listed In XML trigger file " +
                                            "(" + mRunFinishUtc.ToString(CultureInfo.InvariantCulture) + ")";

                            currentTask += "; " + taskAddon;

                            if (TraceMode)
                            {
                                ShowTraceMessage(taskAddon);
                            }

                            var dtFileModDate = File.GetLastWriteTimeUtc(mDatasetPath);

                            var strValue = mMgrParams.GetParam("timevalidationtolerance");
                            if (!int.TryParse(strValue, out var intTimeValToleranceMinutes))
                            {
                                intTimeValToleranceMinutes = 800;
                            }

                            var dtRunFinishWithTolerance = mRunFinishUtc.AddMinutes(intTimeValToleranceMinutes);

                            if (dtFileModDate <= dtRunFinishWithTolerance)
                            {
                                return XmlValidateStatus.XML_VALIDATE_SUCCESS;
                            }

                            var errMsg = "Time validation Error For " + mDatasetName +
                                         " File modification date (UTC): " + dtFileModDate.ToString(CultureInfo.InvariantCulture) +
                                         " vs. Run Finish UTC date " + dtRunFinishWithTolerance.ToString(CultureInfo.InvariantCulture) +
                                         " (includes " + intTimeValToleranceMinutes + " minute tolerance)";

                            clsMainProcess.LogErrorToDatabase(errMsg);
                            return XmlValidateStatus.XML_VALIDATE_FAILED;

                        }
                        break;

                    case RawDsTypes.FolderExt:
                    case RawDsTypes.FolderNoExt:
                        // Dataset found in a folder with an extension
                        // Verify that the directory size is constant
                        currentTask = "Dataset folder found at " + datasetSourcePath + "; verifying folder size is constant for ";

                        // Update the dataset path to include the instrument file or folder name
                        mDatasetPath = Path.Combine(datasetSourcePath, instrumentFileOrFolderName);
                        currentTask += mDatasetPath;
                        if (TraceMode)
                        {
                            ShowTraceMessage(currentTask);
                        }

                        if (!VerifyConstantDirectorySize(mDatasetPath, mSleepInterval))
                        {
                            LogWarning(
                                "Dataset '" + mDatasetName + "' not ready (folder size changed over " + mSleepInterval + " seconds)");

                            if (connected)
                            {
                                currentTask = "Dataset folder size changed; disconnecting from " + mSourcePath;
                                DisconnectShare(mShareConnector);
                            }

                            return XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED;
                        }

                        if (mDatasetPath.EndsWith(".d", StringComparison.OrdinalIgnoreCase))
                        {
                            // Agilent .D dataset
                            // If the AcqData directory has zero-byte .bin files created within the last 60 minutes,
                            // assume the dataset is still being acquired
                            // Example filenames: IMSFrame.bin, MSPeak.bin, MSProfile.bin, and MSScan.bin

                            var acqDataPath = Path.Combine(mDatasetPath, "AcqData");
                            var extensionsToCheck = new List<string>
                            {
                                ".bin"
                            };

                            currentTask = "Checking for zero byte .bin files in " + acqDataPath;
                            if (DirectoryHasRecentZeroByteFiles(acqDataPath, extensionsToCheck, 60))
                            {
                                LogWarning("Dataset '" + mDatasetName + "' not ready (recent zero-byte .bin files)");
                                if (connected)
                                {
                                    currentTask = "Dataset folder size changed; disconnecting from " + mSourcePath;
                                    DisconnectShare(mShareConnector);
                                }

                                return XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED;
                            }

                        }
                        break;

                    default:
                        clsMainProcess.LogErrorToDatabase("Invalid dataset type for " + mDatasetName + ": " + resType);
                        if (connected)
                        {
                            currentTask = "Invalid dataset type; disconnecting from " + mSourcePath;
                            DisconnectShare(mShareConnector);
                        }

                        return XmlValidateStatus.XML_VALIDATE_NO_DATA;
                }

                return XmlValidateStatus.XML_VALIDATE_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("clsXMLTimeValidation.GetInstrumentName(), Error reading XML File, current task: " + currentTask, ex);

                if (ContainsIgnoreCase(ex.Message, "unknown user name or bad password"))
                {
                    // Example message: Error accessing '\\VOrbi05.bionet\ProteomicsData\QC_Shew_11_02_pt5_d2_1Apr12_Earth_12-03-14.raw': Logon failure: unknown user name or bad password
                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE;
                }

                if (ContainsIgnoreCase(ex.Message, "Access to the path") && ContainsIgnoreCase(ex.Message, "is denied"))
                {
                    // Example message: Access to the path '\\exact01.bionet\ProteomicsData\Alz_Cap_Test_14_31Mar12_Roc_12-03-16.raw' is denied.
                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE;
                }

                if (ContainsIgnoreCase(ex.Message, "network path was not found"))
                {
                    // Example message: The network path was not found.
                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR;
                }

                if (ContainsIgnoreCase(ex.Message, "The handle is invalid"))
                {
                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR;
                }

                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;

            }

        }

        /// <summary>
        /// Check for zero byte files in the given directory, optionally filtering by file extension
        /// </summary>
        /// <param name="directoryPath">Directory to check</param>
        /// <param name="extensionsToCheck">List of extensions to check; empty list means check all files</param>
        /// <param name="recentTimeMinutes">Time, in minutes, that is considered "recent" for a zero-byte file</param>
        /// <returns>True if a recent, zero-byte file is found</returns>
        private bool DirectoryHasRecentZeroByteFiles(string directoryPath, IReadOnlyCollection<string> extensionsToCheck, int recentTimeMinutes)
        {
            var dataFolder = new DirectoryInfo(directoryPath);
            if (!dataFolder.Exists)
            {
                return false;
            }

            foreach (var dataFile in dataFolder.GetFiles("*", SearchOption.TopDirectoryOnly))
            {
                // If extensionsToCheck is empty; check all files
                // Otherwise, only check the file if the file extension is in extensionsToCheck

                var checkFile = extensionsToCheck.Any(item => string.Equals(item, dataFile.Extension, StringComparison.OrdinalIgnoreCase));

                if (extensionsToCheck.Count > 0 && !checkFile)
                {
                    continue;
                }

                if (dataFile.Length > 0)
                {
                    continue;
                }

                if (DateTime.UtcNow.Subtract(dataFile.LastWriteTimeUtc).TotalMinutes > recentTimeMinutes)
                {
                    continue;
                }

                LogDebug("Found a recent, zero-byte file: " + dataFile.FullName);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Determines if raw dataset exists as a single file, folder with same name as dataset, or folder with dataset name + extension
        /// </summary>
        /// <param name="instrumentSourcePath"></param>
        /// <param name="captureSubFolderName"></param>
        /// <param name="currentDataset"></param>
        /// <param name="ignoreInstrumentSourceErrors"></param>
        /// <param name="instrumentFileOrFolderName">Output: full name of the dataset file or dataset folder</param>
        /// <returns>Enum specifying what was found</returns>
        private RawDsTypes GetRawDsType(
            string instrumentSourcePath,
            string captureSubFolderName,
            string currentDataset,
            bool ignoreInstrumentSourceErrors,
            out string instrumentFileOrFolderName)
        {
            // Verify instrument source folder exists
            var diSourceFolder = new DirectoryInfo(instrumentSourcePath);
            if (TraceMode)
            {
                ShowTraceMessage("Instantiated diSourceFolder with " + instrumentSourcePath);
            }

            if (!diSourceFolder.Exists)
            {
                var msg = "Source folder not found for dataset " + currentDataset + ": " + diSourceFolder.FullName;
                if (TraceMode)
                {
                    ShowTraceMessage(msg);
                }

                if (ignoreInstrumentSourceErrors)
                {
                    // Simply assume it's a Thermo .raw file
                    instrumentFileOrFolderName = currentDataset + ".raw";
                    return RawDsTypes.File;
                }

                ErrorMessage = msg;
                LogError(ErrorMessage);
                instrumentFileOrFolderName = string.Empty;
                return RawDsTypes.None;
            }

            if (!string.IsNullOrWhiteSpace(captureSubFolderName))
            {
                if (captureSubFolderName.Length > 255)
                {
                    ErrorMessage = "Subdirectory path for dataset " + currentDataset + " is too long (over 255 characters): " + "[" + captureSubFolderName + "]";

                    LogError(ErrorMessage);
                    instrumentFileOrFolderName = string.Empty;
                    return RawDsTypes.None;
                }

                var diSubfolder = new DirectoryInfo(Path.Combine(diSourceFolder.FullName, captureSubFolderName));
                if (!diSubfolder.Exists)
                {
                    ErrorMessage = "Source directory not found for dataset " + currentDataset + " in the given subdirectory: " + "[" + diSubfolder.FullName + "]";

                    LogError(ErrorMessage);
                    instrumentFileOrFolderName = string.Empty;
                    return RawDsTypes.None;
                }

                diSourceFolder = diSubfolder;
            }

            // Check for a file with specified name
            foreach (var fiFile in diSourceFolder.GetFiles())
            {
                if (string.Equals(Path.GetFileNameWithoutExtension(fiFile.Name), currentDataset, StringComparison.OrdinalIgnoreCase))
                {
                    instrumentFileOrFolderName = fiFile.Name;
                    return RawDsTypes.File;
                }
            }

            // Check for a folder with specified name
            foreach (var diFolder in diSourceFolder.GetDirectories())
            {
                if (!string.Equals(Path.GetFileNameWithoutExtension(diFolder.Name), currentDataset, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (diFolder.Extension.Length == 0)
                {
                    // Found a directory that has no extension
                    instrumentFileOrFolderName = diFolder.Name;
                    return RawDsTypes.FolderNoExt;
                }

                // Directory name has an extension
                instrumentFileOrFolderName = diFolder.Name;
                return RawDsTypes.FolderExt;

            }

            // If we got to here, the raw dataset wasn't found, so there was a problem
            instrumentFileOrFolderName = string.Empty;
            return RawDsTypes.None;
        }

        private void DisconnectShare(ShareConnector connector)
        {
            if (TraceMode)
            {
                ShowTraceMessage("Disconnecting from Bionet share");
            }

            // Disconnects a shared drive
            connector.Disconnect();
        }

        private void ShowTraceMessage(string message)
        {
            clsMainProcess.ShowTraceMessage(message);
        }

        /// <summary>
        /// Determines if the size of a folder changes over specified time interval
        /// </summary>
        /// <param name="folderPath"></param>
        /// <param name="sleepIntervalSeconds"></param>
        /// <returns>True if constant, false if changed</returns>
        /// <remarks></remarks>
        private bool VerifyConstantDirectorySize(string folderPath, int sleepIntervalSeconds)
        {
            // Sleep interval should be no more than 15 minutes (900 seconds)
            if (sleepIntervalSeconds > 900)
            {
                sleepIntervalSeconds = 900;
            }

            if (sleepIntervalSeconds < 1)
            {
                sleepIntervalSeconds = 1;
            }

            // Get the initial size of the directory
            var initialDirectorySize = mFileTools.GetDirectorySize(folderPath);

            SleepWhileVerifyingConstantSize(sleepIntervalSeconds, "directory");

            // Get the final size of the directory and compare
            var finalDirectorySize = mFileTools.GetDirectorySize(folderPath);
            if (finalDirectorySize == initialDirectorySize)
            {
                return true;
            }

            return false;

        }

        /// <summary>
        /// Determines if the size of a file changes over specified time interval
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="sleepIntervalSeconds"></param>
        /// <param name="logonFailure"></param>
        /// <returns>True if constant, false if changed</returns>
        /// <remarks></remarks>
        private bool VerifyConstantFileSize(string filePath, int sleepIntervalSeconds, out bool logonFailure)
        {
            // Sleep interval should be no more than 15 minutes (900 seconds)
            if (sleepIntervalSeconds > 900)
            {
                sleepIntervalSeconds = 900;
            }

            if (sleepIntervalSeconds < 1)
            {
                sleepIntervalSeconds = 1;
            }

            logonFailure = false;
            try
            {
                // Get the initial size of the file
                var fiDatasetFile = new FileInfo(filePath);
                var initialFileSize = fiDatasetFile.Length;

                SleepWhileVerifyingConstantSize(sleepIntervalSeconds, "file");

                // Get the final size of the file and compare
                fiDatasetFile.Refresh();
                var finalFileSize = fiDatasetFile.Length;
                if (finalFileSize == initialFileSize)
                {
                    return true;
                }

                return false;

            }
            catch (Exception ex)
            {
                LogWarning("Error accessing: " + filePath + ": " + ex.Message);

                // Check for "Logon failure: unknown user name or bad password."

                if (ContainsIgnoreCase(ex.Message, "unknown user name or bad password"))
                {
                    // This error occasionally occurs when monitoring a .UIMF file on an IMS instrument
                    // We'll treat this as an indicator that the file size is not constant
                    if (TraceMode)
                    {
                        ShowTraceMessage("Error message contains 'unknown user name or bad password'; assuming this means the file size is not " +
                            "constant");
                    }

                    logonFailure = true;
                }
                else
                {
                    // Note that error "The user name or password is incorrect" could be due to the Secondary Logon service not running
                    // We check for that in clsProcessXmlTriggerFile.ProcessFile if ValidateXMLFileMain returns false
                    throw;
                }

            }

            return false;
        }

        private bool ContainsIgnoreCase(string textToSearch, string textToFind)
        {
            if (textToSearch.ToLower().Contains(textToFind.ToLower()))
            {
                return true;
            }

            return false;
        }

        private bool SetOperatorName()
        {
            try
            {
                if (string.IsNullOrWhiteSpace(mOperatorUsername))
                {
                    var logMsg = "clsXMLTimeValidation.SetOperatorName: Operator field is empty (should be a network login, e.g. D3E154)";
                    LogWarning(logMsg);
                    mOperatorName = logMsg;
                    return false;
                }

                var operatorInfo = mDMSInfoCache.GetOperatorName(mOperatorUsername, out var userCountMatched);

                // Update the operator name, e-mail, and PRN
                mOperatorName = operatorInfo.Name;
                mOperatorEmail = operatorInfo.Email;
                mOperatorUsername = operatorInfo.Username;

                if (userCountMatched == 1)
                {
                    // We matched a single user using strQueryName
                    return true;
                }

                // We matched 0 users, or more than one user
                // An error should have already been logged by mDMSInfoCache
                return false;

            }
            catch (Exception ex)
            {
                LogError("clsXMLTimeValidation.RetrieveOperatorName(), Error retrieving Operator Name", ex);
                return false;
            }

        }

        private void SleepWhileVerifyingConstantSize(int sleepIntervalSeconds, string datasetType)
        {
            int actualSecondsToSleep;

            if (TraceMode)
            {
                if (sleepIntervalSeconds > 3)
                {
                    actualSecondsToSleep = 3;
                    ShowTraceMessage("Monitoring dataset " + datasetType + " for 3 seconds to see if its size changes " +
                        "(would wait " + sleepIntervalSeconds + " seconds if PreviewMode was not enabled)");
                }
                else
                {
                    actualSecondsToSleep = sleepIntervalSeconds;
                    ShowTraceMessage("Monitoring dataset " + datasetType + " for " + sleepIntervalSeconds + " seconds to see if its size changes");
                }

            }
            else
            {
                actualSecondsToSleep = sleepIntervalSeconds;
            }

            // Wait for specified sleep interval
            ConsoleMsgUtils.SleepSeconds(actualSecondsToSleep);
        }
    }
}
