using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using PRISM;
using PRISM.AppSettings;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class XMLTimeValidation : LoggerBase
    {
        // Ignore Spelling: AcqData, Alz, bionet, dms, fso, logon, prepend, pwd, Roc, secfso, subfolder, username

        private string mDatasetName = string.Empty;

        private readonly DateTime mDefaultRunFinishUtc = new(1960, 1, 1);

        private DateTime mRunFinishUtc;

        private string mCaptureType = string.Empty;

        private string mSourcePath = string.Empty;

        private readonly Dictionary<char, string> mFilenameAutoFixes;

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

        private readonly ProcessDatasetInfoBase.XmlProcSettingsType mProcSettings;

        private readonly ConcurrentDictionary<string, int> mInstrumentsToSkip;

        private readonly FileTools mFileTools;

        private readonly MgrSettings mMgrParams;

        private string captureShareNameInTriggerFile = string.Empty;

        /// <summary>
        /// Capture subdirectory
        /// </summary>
        public string CaptureSubdirectory { get; private set; } = string.Empty;

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName => FixNull(mDatasetName);

        /// <summary>
        /// Source path to the dataset on the instrument
        /// </summary>
        public string DatasetPath => FixNull(mDatasetPath);

        /// <summary>
        /// Instrument name
        /// </summary>
        public string InstrumentName { get; private set; } = string.Empty;

        /// <summary>
        /// Time validation error message
        /// </summary>
        public string ErrorMessage { get; private set; } = string.Empty;

        /// <summary>
        /// Instrument operator's e-mail
        /// </summary>
        public string OperatorEMail => FixNull(mOperatorEmail);

        /// <summary>
        /// Instrument operator's name
        /// </summary>
        public string OperatorName => FixNull(mOperatorName);

        /// <summary>
        /// Source path on the instrument, e.g. \\TSQ_1\ProteomicsData
        /// </summary>
        public string SourcePath => FixNull(mSourcePath);

        /// <summary>
        /// When true, show additional messages
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// Dataset types
        /// </summary>
        private enum RawDsTypes
        {
            None = 0,
            File = 1,
            DirectoryNoExtension = 2,
            DirectoryWithExtension = 3
        }

        /// <summary>
        /// Validation status
        /// </summary>
        public enum XmlValidateStatus
        {
            // ReSharper disable InconsistentNaming
            XML_VALIDATE_SUCCESS = 0,
            XML_VALIDATE_FAILED = 1,
            // XML_VALIDATE_NO_CHECK = 2,
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

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="instrumentsToSkip"></param>
        /// <param name="dmsCache"></param>
        /// <param name="settings"></param>
        public XMLTimeValidation(
            MgrSettings mgrParams,
            ConcurrentDictionary<string, int> instrumentsToSkip,
            DMSInfoCache dmsCache,
            ProcessDatasetInfoBase.XmlProcSettingsType settings)
        {
            mMgrParams = mgrParams;
            mFileTools = new FileTools();
            mInstrumentsToSkip = instrumentsToSkip;
            mDMSInfoCache = dmsCache;
            mProcSettings = settings;

            mFilenameAutoFixes = new Dictionary<char, string> {
                { ' ', "_"},
                { '%', "pct"},
                { '.', "pt"}};

            mRunFinishUtc = mDefaultRunFinishUtc;
        }

        /// <summary>
        /// Look for textToFind in textToSearch, ignoring case
        /// </summary>
        /// <param name="textToSearch"></param>
        /// <param name="textToFind"></param>
        private bool ContainsIgnoreCase(string textToSearch, string textToFind)
        {
            return textToSearch.IndexOf(textToFind, StringComparison.OrdinalIgnoreCase) >= 0;
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
            var dataDirectory = new DirectoryInfo(directoryPath);

            if (!dataDirectory.Exists)
            {
                return false;
            }

            foreach (var dataFile in dataDirectory.GetFiles("*", SearchOption.TopDirectoryOnly))
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
        /// Disconnect from a remote share
        /// </summary>
        /// <param name="connector"></param>
        private void DisconnectShare(ShareConnector connector)
        {
            if (TraceMode)
            {
                ShowTraceMessage("Disconnecting from Bionet share");
            }

            connector?.Disconnect();
        }

        /// <summary>
        /// Look for a directory named datasetName, optionally replacing invalid characters
        /// </summary>
        /// <param name="datasetName">Dataset name to find</param>
        /// <param name="sourceDirectory">Remote directory to search</param>
        /// <param name="replaceInvalidCharacters">When true, replace invalid characters with a substitute</param>
        /// <param name="matchingDirectory">Output: directory object, if a match</param>
        /// <param name="hasExtension">Output: true if a match was found and it has an extension</param>
        /// <returns>True if a match is found, otherwise false</returns>
        private bool FindDatasetDirectory(
            string datasetName,
            DirectoryInfo sourceDirectory,
            bool replaceInvalidCharacters,
            out DirectoryInfo matchingDirectory,
            out bool hasExtension)
        {
            hasExtension = false;

            // Check for a directory with specified name
            foreach (var remoteDirectory in sourceDirectory.GetDirectories())
            {
                var baseName = Path.GetFileNameWithoutExtension(remoteDirectory.Name);

                var remoteDirectoryName = replaceInvalidCharacters ? ReplaceInvalidChars(baseName) : baseName;

                if (!string.Equals(remoteDirectoryName, datasetName, StringComparison.OrdinalIgnoreCase))
                    continue;

                ShowTraceMessage(string.Format("Matched remote directory '{0}'", remoteDirectoryName));

                if (replaceInvalidCharacters && !remoteDirectoryName.Equals(baseName))
                {
                    LogMessage(string.Format(
                                   "Remote dataset name has spaces; directory will be renamed during capture: '{0}'",
                                   remoteDirectory.Name));
                }

                if (remoteDirectory.Extension.Length == 0)
                {
                    // Found a directory that has no extension
                    matchingDirectory = remoteDirectory;
                    return true;
                }

                // Directory name has an extension
                hasExtension = true;
                matchingDirectory = remoteDirectory;
                return true;
            }

            matchingDirectory = null;
            return false;
        }

        /// <summary>
        /// Look for a file named datasetName, optionally replacing invalid characters
        /// </summary>
        /// <param name="datasetName">Dataset name to find</param>
        /// <param name="sourceDirectory">Remote directory to search</param>
        /// <param name="replaceInvalidCharacters">When true, replace invalid characters with a substitute</param>
        /// <param name="matchingFile">Output: file object, if a match</param>
        /// <returns>True if a match is found, otherwise false</returns>
        private bool FindDatasetFile(
            string datasetName,
            DirectoryInfo sourceDirectory,
            bool replaceInvalidCharacters,
            out FileInfo matchingFile)
        {
            foreach (var remoteFile in sourceDirectory.GetFiles())
            {
                var baseName = Path.GetFileNameWithoutExtension(remoteFile.Name);

                var remoteFileName = replaceInvalidCharacters ? ReplaceInvalidChars(baseName) : baseName;

                if (!string.Equals(remoteFileName, datasetName, StringComparison.OrdinalIgnoreCase))
                    continue;

                ShowTraceMessage(string.Format("Matched remote file '{0}'", remoteFileName));

                if (replaceInvalidCharacters && !remoteFileName.Equals(baseName))
                {
                    LogMessage(string.Format(
                                   "Remote dataset name has spaces; file will be renamed during capture: '{0}'",
                                   remoteFile.Name));
                }

                matchingFile = remoteFile;
                return true;
            }

            matchingFile = null;
            return false;
        }

        /// <summary>
        /// If textToCheck is null or empty, return an empty string
        /// </summary>
        /// <param name="textToCheck"></param>
        private static string FixNull(string textToCheck)
        {
            return string.IsNullOrWhiteSpace(textToCheck) ? string.Empty : textToCheck;
        }

        /// <summary>
        /// Determines if raw dataset exists as a single file, directory with same name as dataset, or directory with dataset name + extension
        /// </summary>
        /// <param name="instrumentSourcePath"></param>
        /// <param name="captureSubFolderName"></param>
        /// <param name="currentDataset"></param>
        /// <param name="ignoreInstrumentSourceErrors"></param>
        /// <param name="instrumentFileOrDirectoryName">Output: full name of the dataset file or dataset directory</param>
        /// <returns>Enum specifying what was found</returns>
        private RawDsTypes GetRawDsType(
            string instrumentSourcePath,
            string captureSubFolderName,
            string currentDataset,
            bool ignoreInstrumentSourceErrors,
            out string instrumentFileOrDirectoryName)
        {
            // Verify instrument source directory exists
            var sourceDirectory = new DirectoryInfo(instrumentSourcePath);

            if (TraceMode)
            {
                ShowTraceMessage("Instantiated sourceDirectory with " + instrumentSourcePath);
            }

            if (!sourceDirectory.Exists)
            {
                var msg = "Source directory not found for dataset " + currentDataset + ": " + sourceDirectory.FullName;

                if (TraceMode)
                {
                    ShowTraceMessage(msg);
                }

                if (ignoreInstrumentSourceErrors)
                {
                    // Simply assume it's a Thermo .raw file
                    instrumentFileOrDirectoryName = currentDataset + ".raw";
                    return RawDsTypes.File;
                }

                ErrorMessage = msg;
                LogError(ErrorMessage);
                instrumentFileOrDirectoryName = string.Empty;
                return RawDsTypes.None;
            }

            if (!string.IsNullOrWhiteSpace(captureSubFolderName))
            {
                if (captureSubFolderName.Length > 255)
                {
                    ErrorMessage = string.Format(
                        "Subdirectory path for dataset {0} is too long (over 255 characters): [{1}]",
                        currentDataset, captureSubFolderName);

                    LogError(ErrorMessage);
                    instrumentFileOrDirectoryName = string.Empty;
                    return RawDsTypes.None;
                }

                var subdirectory = new DirectoryInfo(Path.Combine(sourceDirectory.FullName, captureSubFolderName));

                if (!subdirectory.Exists)
                {
                    ErrorMessage = string.Format(
                        "Source directory not found for dataset {0} in the given subdirectory: [{1}]",
                        currentDataset, subdirectory.FullName);

                    LogError(ErrorMessage);
                    instrumentFileOrDirectoryName = string.Empty;
                    return RawDsTypes.None;
                }

                sourceDirectory = subdirectory;
            }

            // When i = 0, check for an exact match to a file or directory
            // When i = 1, replace spaces with underscores and try again to match

            for (var i = 0; i < 2; i++)
            {
                var replaceInvalidCharacters = (i > 0);

                if (FindDatasetFile(currentDataset, sourceDirectory, replaceInvalidCharacters, out var matchingFile))
                {
                    instrumentFileOrDirectoryName = matchingFile.Name;
                    return RawDsTypes.File;
                }

                if (FindDatasetDirectory(currentDataset, sourceDirectory, replaceInvalidCharacters, out var matchingDirectory, out var hasExtension))
                {
                    if (hasExtension)
                    {
                        instrumentFileOrDirectoryName = matchingDirectory.Name;
                        return RawDsTypes.DirectoryNoExtension;
                    }

                    instrumentFileOrDirectoryName = matchingDirectory.Name;
                    return RawDsTypes.DirectoryWithExtension;
                }
            }

            // If we got to here, the raw dataset wasn't found, so there was a problem
            instrumentFileOrDirectoryName = string.Empty;
            return RawDsTypes.None;
        }

        /// <summary>
        /// Extract certain settings from dataset creation task XML
        /// </summary>
        /// <param name="createTaskInfo"></param>
        private XmlValidateStatus GetXmlParameters(DatasetCreateTaskInfo createTaskInfo)
        {
            try
            {
                var doc = XDocument.Parse(createTaskInfo.XmlParameters);
                var elements = doc.Elements("root").ToList();

                InstrumentName = DatasetCreateTaskInfo.GetXmlValue(elements, createTaskInfo.CreateTaskXmlNames[DatasetCaptureInfo.DatasetMetadata.Instrument]);

                createTaskInfo.CaptureShareName = DatasetCreateTaskInfo.GetXmlValue(elements, createTaskInfo.CreateTaskXmlNames[DatasetCaptureInfo.DatasetMetadata.CaptureShareName]);

                CaptureSubdirectory = DatasetCreateTaskInfo.GetXmlValue(elements, createTaskInfo.CreateTaskXmlNames[DatasetCaptureInfo.DatasetMetadata.CaptureSubdirectory]);

                ValidateCaptureSubdirectory(createTaskInfo);

                mDatasetName = DatasetCreateTaskInfo.GetXmlValue(elements, createTaskInfo.CreateTaskXmlNames[DatasetCaptureInfo.DatasetMetadata.Dataset]);

                mOperatorUsername = DatasetCreateTaskInfo.GetXmlValue(elements, createTaskInfo.CreateTaskXmlNames[DatasetCaptureInfo.DatasetMetadata.OperatorUsername]);

                if (!string.IsNullOrWhiteSpace(InstrumentName))
                    return XmlValidateStatus.XML_VALIDATE_CONTINUE;

                LogError("XMLTimeValidation.GetXMLParameters(), The instrument name was blank.");
                return XmlValidateStatus.XML_VALIDATE_BAD_XML;
            }
            catch (Exception ex)
            {
                LogError("XMLTimeValidation.GetXMLParameters(), Error parsing dataset creation task XML", ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }
        }

        /// <summary>
        /// Extract certain settings from an XML file
        /// </summary>
        /// <param name="triggerFileInfo"></param>
        private XmlValidateStatus GetXmlParameters(TriggerFileInfo triggerFileInfo)
        {
            var xmlFileContents = Global.LoadXmlFileContentsIntoString(triggerFileInfo.TriggerFile);

            if (string.IsNullOrWhiteSpace(xmlFileContents))
            {
                return XmlValidateStatus.XML_VALIDATE_TRIGGER_FILE_MISSING;
            }

            return GetXmlParameters(triggerFileInfo, xmlFileContents);
        }

        /// <summary>
        /// Extract certain settings from dataset capture XML
        /// </summary>
        /// <param name="captureInfo">Dataset capture info</param>
        /// <param name="xmlFileContents"></param>
        private XmlValidateStatus GetXmlParameters(DatasetCaptureInfo captureInfo, string xmlFileContents)
        {
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

                            case "Capture Share Name":
                                captureShareNameInTriggerFile = row["Value"].ToString();
                                captureInfo.CaptureShareName = captureShareNameInTriggerFile;
                                break;

                            case "Capture Subfolder":
                            case "Capture Subdirectory":
                                CaptureSubdirectory = row["Value"].ToString();

                                ValidateCaptureSubdirectory(captureInfo);

                                break;

                            case "Dataset Name":
                                mDatasetName = row["Value"].ToString();
                                break;

                            case "Run Finish UTC":
                                mRunFinishUtc = DateTime.Parse(row["Value"].ToString());
                                break;

                            case "Operator (Username)":
                            case "Operator (PRN)":
                                mOperatorUsername = row["Value"].ToString();
                                break;
                        }
                    }
                }

                if (string.IsNullOrWhiteSpace(InstrumentName))
                {
                    LogError("XMLTimeValidation.GetXMLParameters(), The instrument name was blank.");
                    return XmlValidateStatus.XML_VALIDATE_BAD_XML;
                }

                // Prior to October 2023, this program would wait 10 minutes before processing XML trigger files for certain instruments

                // var validationResult = XmlValidateStatus.XML_VALIDATE_CONTINUE;

                // if (InstrumentName.StartsWith("9T") ||
                //     InstrumentName.StartsWith("11T") ||
                //     InstrumentName.StartsWith("12T"))
                // {
                //     validationResult = InstrumentWaitDelay(triggerFileInfo.TriggerFile);
                // }

                // if (validationResult != XmlValidateStatus.XML_VALIDATE_CONTINUE)
                // {
                //     return validationResult;
                // }

                return XmlValidateStatus.XML_VALIDATE_CONTINUE;
            }
            catch (Exception ex)
            {
                LogError("XMLTimeValidation.GetXMLParameters(), Error reading XML File", ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }
        }

        /// <summary>
        /// Examine the date of the trigger file; if less than XMLFileDelay minutes old, delay processing trigger file
        /// </summary>
        /// <param name="triggerFile"></param>
        // ReSharper disable once UnusedMember.Local
        [Obsolete("Unused")]
        private XmlValidateStatus InstrumentWaitDelay(FileSystemInfo triggerFile)
        {
            try
            {
                // Manager parameter XMLFileDelay is the number of minutes to wait until a newly created file will be processed; typically 10 minutes

                var delayValue = int.Parse(mMgrParams.GetParam("XMLFileDelay"));
                var fileModDate = triggerFile.LastWriteTimeUtc;
                var fileModDateDelay = fileModDate.AddMinutes(delayValue);
                var dateNow = DateTime.UtcNow;

                if (dateNow >= fileModDateDelay)
                    return XmlValidateStatus.XML_VALIDATE_CONTINUE;

                LogWarning(string.Format(
                    "XMLTimeValidation.InstrumentWaitDelay(), dataset import is being delayed until {0:hh:mm:ss tt} for XML File: {1}",
                    triggerFile.LastWriteTime.AddMinutes(delayValue), triggerFile.Name));

                return XmlValidateStatus.XML_WAIT_FOR_FILES;
            }
            catch (Exception ex)
            {
                LogError("XMLTimeValidation.InstrumentWaitDelay(), error determining wait delay", ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }
        }

        /// <summary>
        /// Validate that the remote dataset exists and that its size is constant
        /// </summary>
        private XmlValidateStatus PerformValidation()
        {
            var connected = false;
            var currentTask = string.Empty;
            var ignoreInstrumentSourceErrors = mProcSettings.IgnoreInstrumentSourceErrors;

            // method-local versions of sourcePath and capture subdirectory, to support use of alternate shares
            var sourcePath = mSourcePath;
            var subdirectory = CaptureSubdirectory;

            if (!string.IsNullOrWhiteSpace(captureShareNameInTriggerFile))
            {
                // Get the default share name in DMS
                var defaultShareName = mSourcePath.Substring(mSourcePath.IndexOf('\\', 2)).Trim('\\');
                var captureShareName = captureShareNameInTriggerFile.Trim('\\', '.');

                if (!defaultShareName.Trim('\\', '.').Split().Last().Equals(captureShareName, StringComparison.OrdinalIgnoreCase))
                {
                    // If working on a share, special handling to allow replacing the share name
                    // .NET is smart and considers '\\server_name\share_name' as the path root, so '..\' doesn't work
                    if (sourcePath.StartsWith("\\\\"))
                    {
                        var tempSource = sourcePath.Trim('\\', '.').Split('\\')[0];
                        sourcePath = "\\\\" + Path.Combine(tempSource, captureShareName);
                    }

                    captureShareName = $"..\\{captureShareName}";

                    // If mSourcePath specifies more than just a host name and share name (that is, it also specifies a subdirectory)
                    // we need to add additional directory backups
                    var extraDirectoryBackups = defaultShareName.Count(x => x == '\\');

                    for (var i = 0; i < extraDirectoryBackups; i++)
                    {
                        captureShareName = $"..\\{captureShareName}";
                    }

                    if (string.IsNullOrWhiteSpace(CaptureSubdirectory))
                    {
                        CaptureSubdirectory = captureShareName;
                    }
                    else
                    {
                        CaptureSubdirectory = Path.Combine(captureShareName, CaptureSubdirectory);
                    }
                }
            }

            try
            {
                if (string.IsNullOrWhiteSpace(mCaptureType) || string.IsNullOrWhiteSpace(mSourcePath))
                {
                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
                }

                if (string.Equals(mCaptureType, "secfso", StringComparison.OrdinalIgnoreCase))
                {
                    // Make sure mSourcePath is not of the form \\proto-2 because if that is the case, mCaptureType should be "fso"
                    var reProtoServer = new Regex(@"\\\\proto-\d+\\", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    if (reProtoServer.IsMatch(mSourcePath))
                    {
                        // Auto-change mCaptureType to "fso", and log an error in the database
                        mCaptureType = "fso";

                        var errMsg = string.Format("Instrument {0} is configured to use 'secfso' " +
                                                   "yet its source directory is {1}, which appears to be a domain path; " +
                                                   "auto-changing the capture_method to 'fso' for now, but the configuration " +
                                                   "in the database should be updated (see table T_Instrument_Name)",
                                                   InstrumentName, mSourcePath);

                        if (TraceMode)
                        {
                            Console.WriteLine(" - - - - - - - - ");
                            ShowTraceMessage("ERROR: " + errMsg);
                            Console.WriteLine(" - - - - - - - - ");
                        }

                        MainProcess.LogErrorToDatabase(errMsg);
                    }
                }

                // Define the source path now, before attempting to connect to Bionet
                // This is done so that mDatasetPath will be defined, so we can include it in a log message if a connection error occurs
                string datasetSourcePath;

                if (string.IsNullOrWhiteSpace(subdirectory))
                {
                    datasetSourcePath = string.Copy(sourcePath);
                }
                else
                {
                    datasetSourcePath = Path.Combine(sourcePath, subdirectory);
                }

                // Initially define this as the dataset source directory and the dataset name
                // It will later be updated to have the actual instrument file or directory name
                mDatasetPath = Path.Combine(datasetSourcePath, mDatasetName);

                if (string.Equals(mCaptureType, "secfso", StringComparison.OrdinalIgnoreCase) &&
                    !Global.GetHostName().Equals(MainProcess.DEVELOPER_COMPUTER_NAME, StringComparison.OrdinalIgnoreCase))
                {
                    // Source directory is on bionet; establish a connection
                    var username = mMgrParams.GetParam("BionetUser");
                    var encodedPwd = mMgrParams.GetParam("BionetPwd");

                    if (!username.Contains('\\'))
                    {
                        // Prepend this computer's name to the username
                        username = Global.GetHostName() + '\\' + username;
                    }

                    var currentTaskBase = string.Format(
                        "Connecting to {0} using secfso, user {1}, and encoded password {2}",
                        sourcePath, username, encodedPwd);

                    currentTask = currentTaskBase + "; Decoding password";

                    if (TraceMode)
                    {
                        ShowTraceMessage(currentTask);
                    }

                    var decodedPwd = AppUtils.DecodeShiftCipher(encodedPwd);

                    currentTask = currentTaskBase + "; Instantiating ShareConnector";

                    if (TraceMode)
                    {
                        ShowTraceMessage(currentTask);
                    }

                    mShareConnector = new ShareConnector(username, decodedPwd)
                    {
                        Share = sourcePath
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
                        ErrorMessage = string.Format(
                            "Error connecting to {0} as user {1} using 'secfso': {2}",
                            sourcePath, username, mShareConnector.ErrorMessage);

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
                                var statusMsg = string.Format(
                                    "Likely had error 'An unexpected network error occurred' while validating the Dataset specified by the XML file (ErrorMessage={0})",
                                    mShareConnector.ErrorMessage);

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

                var resType = GetRawDsType(sourcePath, subdirectory, mDatasetName, ignoreInstrumentSourceErrors, out var instrumentFileOrDirectoryName);

                currentTask = "Validating operator name " + mOperatorUsername + " for " + mDatasetName + " at " + datasetSourcePath;

                if (TraceMode)
                {
                    ShowTraceMessage(currentTask);
                }

                if (!SetOperatorName())
                {
                    if (connected)
                    {
                        currentTask = "Operator not found; disconnecting from " + sourcePath;

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
                        // No raw dataset file or directory found
                        currentTask = "Dataset not found at " + datasetSourcePath;

                        if (TraceMode)
                        {
                            ShowTraceMessage(currentTask);
                        }

                        // Disconnect from Bionet if necessary
                        if (connected)
                        {
                            currentTask = "Dataset not found; disconnecting from " + sourcePath;

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

                        // Update the dataset path to include the instrument file or directory name
                        mDatasetPath = Path.Combine(datasetSourcePath, instrumentFileOrDirectoryName);

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
                                currentTask = "Dataset size changed; disconnecting from " + sourcePath;

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

                        if (Global.GetHostName().Equals(MainProcess.DEVELOPER_COMPUTER_NAME, StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine("Skipping date validation because host name starts with {0}", MainProcess.DEVELOPER_COMPUTER_NAME);
                        }
                        else if (mRunFinishUtc != mDefaultRunFinishUtc)
                        {
                            var additionalInfo = "validating file Date vs. Run_Finish listed In XML trigger file " +
                                            "(" + mRunFinishUtc.ToString(CultureInfo.InvariantCulture) + ")";

                            currentTask += "; " + additionalInfo;

                            if (TraceMode)
                            {
                                ShowTraceMessage(additionalInfo);
                            }

                            var dtFileModDate = File.GetLastWriteTimeUtc(mDatasetPath);

                            var value = mMgrParams.GetParam("TimeValidationTolerance");

                            if (!int.TryParse(value, out var timeValToleranceMinutes))
                            {
                                timeValToleranceMinutes = 800;
                            }

                            var dtRunFinishWithTolerance = mRunFinishUtc.AddMinutes(timeValToleranceMinutes);

                            if (dtFileModDate <= dtRunFinishWithTolerance)
                            {
                                return XmlValidateStatus.XML_VALIDATE_SUCCESS;
                            }

                            var errMsg = "Time validation Error For " + mDatasetName +
                                         " File modification date (UTC): " + dtFileModDate.ToString(CultureInfo.InvariantCulture) +
                                         " vs. Run Finish UTC date " + dtRunFinishWithTolerance.ToString(CultureInfo.InvariantCulture) +
                                         " (includes " + timeValToleranceMinutes + " minute tolerance)";

                            MainProcess.LogErrorToDatabase(errMsg);
                            return XmlValidateStatus.XML_VALIDATE_FAILED;
                        }
                        break;

                    case RawDsTypes.DirectoryWithExtension:
                    case RawDsTypes.DirectoryNoExtension:
                        // Dataset found in a directory with an extension
                        // Verify that the directory size is constant
                        currentTask = "Dataset directory found at " + datasetSourcePath + "; verifying directory size is constant for ";

                        // Update the dataset path to include the instrument file or directory name
                        mDatasetPath = Path.Combine(datasetSourcePath, instrumentFileOrDirectoryName);

                        currentTask += mDatasetPath;

                        if (TraceMode)
                        {
                            ShowTraceMessage(currentTask);
                        }

                        if (!VerifyConstantDirectorySize(mDatasetPath, mSleepInterval))
                        {
                            LogWarning(
                                "Dataset '" + mDatasetName + "' not ready (directory size changed over " + mSleepInterval + " seconds)");

                            if (connected)
                            {
                                currentTask = "Dataset directory size changed; disconnecting from " + sourcePath;
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
                                    currentTask = "Dataset directory size changed; disconnecting from " + sourcePath;
                                    DisconnectShare(mShareConnector);
                                }

                                return XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED;
                            }
                        }
                        break;

                    default:
                        MainProcess.LogErrorToDatabase("Invalid dataset type for " + mDatasetName + ": " + resType);

                        if (connected)
                        {
                            currentTask = "Invalid dataset type; disconnecting from " + sourcePath;
                            DisconnectShare(mShareConnector);
                        }

                        return XmlValidateStatus.XML_VALIDATE_NO_DATA;
                }

                return XmlValidateStatus.XML_VALIDATE_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("XMLTimeValidation.GetInstrumentName(), Error reading XML File, current task: " + currentTask, ex);

                if (ContainsIgnoreCase(ex.Message, "unknown user name or bad password"))
                {
                    // ReSharper disable once CommentTypo
                    // ReSharper disable once GrammarMistakeInComment
                    // Example message: Error accessing '\\VOrbi05.bionet\ProteomicsData\QC_Shew_11_02_pt5_d2_1Apr12_Earth_12-03-14.raw': Logon failure: unknown user name or bad password
                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE;
                }

                if (ContainsIgnoreCase(ex.Message, "user name or password is incorrect"))
                {
                    // ReSharper disable once CommentTypo
                    // ReSharper disable once GrammarMistakeInComment
                    // Example message: Error reading XML File, current task: Dataset found at \\QEHFX01.bionet\ProteomicsData\; verifying file size is constant: The user name or password is incorrect.
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
        /// Auto change spaces to underscores, % to 'pct', and periods to 'pt' in the search text
        /// </summary>
        /// <param name="searchText"></param>
        private string ReplaceInvalidChars(string searchText)
        {
            var updatedText = string.Copy(searchText);

            foreach (var charToFind in mFilenameAutoFixes)
            {
                updatedText = updatedText.Replace(charToFind.Key.ToString(), charToFind.Value);
            }

            return updatedText;
        }

        /// <summary>
        /// Query to get the instrument data from the database and then iterate through the dataset to retrieve the capture type and source path
        /// </summary>
        /// <param name="instrumentName"></param>
        private XmlValidateStatus SetDbInstrumentParameters(string instrumentName)
        {
            try
            {
                // Query the database to obtain information about the given instrument

                if (!mDMSInfoCache.GetInstrumentInfo(instrumentName, out var instrumentInfo))
                {
                    LogError(
                        "XMLTimeValidation.SetDbInstrumentParameters(), Instrument " +
                        instrumentName + " not found in data from V_Instrument_List_Export");

                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
                }

                mCaptureType = instrumentInfo.CaptureType;
                mSourcePath = instrumentInfo.SourcePath;

                if (string.IsNullOrWhiteSpace(mCaptureType))
                {
                    MainProcess.LogErrorToDatabase(
                        "XMLTimeValidation.SetDbInstrumentParameters(), Instrument " +
                        instrumentName + " has an empty value for Capture in V_Instrument_List_Export");

                    return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
                }

                if (!string.IsNullOrWhiteSpace(mSourcePath))
                    return XmlValidateStatus.XML_VALIDATE_CONTINUE;

                MainProcess.LogErrorToDatabase(
                    "XMLTimeValidation.SetDbInstrumentParameters(), Instrument " + instrumentName +
                    " has an empty value for SourcePath in V_Instrument_List_Export");

                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }
            catch (Exception ex)
            {
                LogError(
                    "XMLTimeValidation.SetDbInstrumentParameters(), " +
                    "Error retrieving source path and capture type for instrument: " + instrumentName, ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }
        }

        /// <summary>
        /// Validate that the instrument operator is defined, and matches a user in DMS
        /// </summary>
        private bool SetOperatorName()
        {
            try
            {
                if (string.IsNullOrWhiteSpace(mOperatorUsername))
                {
                    const string logMsg = "XMLTimeValidation.SetOperatorName: Operator field is empty (should be a network login, e.g. D3E154)";
                    LogWarning(logMsg);
                    mOperatorName = logMsg;
                    return false;
                }

                var operatorInfo = mDMSInfoCache.GetUserInfo(mOperatorUsername, out var userCountMatched);

                // Update the operator name, e-mail, and PRN
                mOperatorName = operatorInfo.Name;
                mOperatorEmail = operatorInfo.Email;
                mOperatorUsername = operatorInfo.Username;

                if (userCountMatched == 1)
                {
                    // We matched a single user
                    return true;
                }

                // We matched 0 users, or more than one user
                // An error should have already been logged by mDMSInfoCache
                return false;
            }
            catch (Exception ex)
            {
                LogError("XMLTimeValidation.RetrieveOperatorName(), Error retrieving Operator Name", ex);
                return false;
            }
        }

        /// <summary>
        /// Show a trace message
        /// </summary>
        /// <param name="message"></param>
        private void ShowTraceMessage(string message)
        {
            MainProcess.ShowTraceMessage(message);
        }

        /// <summary>
        /// Sleep for up to 3 seconds
        /// </summary>
        /// <param name="sleepIntervalSeconds"></param>
        /// <param name="datasetType"></param>
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

        private void ValidateCaptureSubdirectory(DatasetCaptureInfo captureInfo)
        {
            if (Path.IsPathRooted(CaptureSubdirectory))
            {
                // Instrument directory has an older version of Buzzard that incorrectly determines the capture subfolder
                // For safety, will blank this out, but will post a log entry to the database
                var msg = "XMLTimeValidation.GetXMLParameters(), the CaptureSubfolder is not a relative path; " +
                          "this indicates a bug with Buzzard; see: " + captureInfo.GetSourceDescription();

                LogError(msg, null, true);
                CaptureSubdirectory = string.Empty;
            }

            captureInfo.OriginalCaptureSubdirectory = CaptureSubdirectory;
        }

        /// <summary>
        /// Process dataset creation task XML
        /// </summary>
        /// <param name="createTaskInfo"></param>
        public XmlValidateStatus ValidateDatasetCreateTaskXml(DatasetCreateTaskInfo createTaskInfo)
        {
            ErrorMessage = string.Empty;
            try
            {
                var xmlLoadResult = GetXmlParameters(createTaskInfo);

                // ReSharper disable once ConvertIfStatementToReturnStatement
                if (xmlLoadResult != XmlValidateStatus.XML_VALIDATE_CONTINUE)
                {
                    return xmlLoadResult;
                }

                return ValidateXmlFileWork(createTaskInfo);
            }
            catch (Exception ex)
            {
                ErrorMessage = string.Format("Error validating XML for dataset create task {0}", createTaskInfo.TaskID);
                var errMsg = "XMLTimeValidation.ValidateDatasetCreateTaskXml(), " + ErrorMessage + ": " + ex.Message;
                LogError(errMsg);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }
        }

        /// <summary>
        /// Process an XML file that defines a new dataset to add to DMS
        /// </summary>
        /// <param name="triggerFileInfo">XML file to process</param>
        public XmlValidateStatus ValidateXmlFile(TriggerFileInfo triggerFileInfo)
        {
            ErrorMessage = string.Empty;
            var triggerFile = triggerFileInfo.TriggerFile;

            try
            {
                if (TraceMode)
                {
                    ShowTraceMessage("Reading " + triggerFile.FullName);
                }

                var xmlLoadResult = GetXmlParameters(triggerFileInfo);

                // ReSharper disable once ConvertIfStatementToReturnStatement
                if (xmlLoadResult != XmlValidateStatus.XML_VALIDATE_CONTINUE)
                {
                    return xmlLoadResult;
                }

                return ValidateXmlFileWork(triggerFileInfo);
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error reading the XML file " + triggerFile.Name;
                var errMsg = "XMLTimeValidation.ValidateXMLFile(), " + ErrorMessage + ": " + ex.Message;
                LogError(errMsg);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }
        }

        /// <summary>
        /// Process XML that defines a new dataset to add to DMS
        /// </summary>
        /// <param name="captureInfo">Dataset capture info</param>
        private XmlValidateStatus ValidateXmlFileWork(DatasetCaptureInfo captureInfo)
        {
            if (mInstrumentsToSkip.ContainsKey(InstrumentName))
            {
                return XmlValidateStatus.XML_VALIDATE_SKIP_INSTRUMENT;
            }

            try
            {
                var instrumentValidationResult = SetDbInstrumentParameters(InstrumentName);

                if (instrumentValidationResult != XmlValidateStatus.XML_VALIDATE_CONTINUE)
                {
                    return instrumentValidationResult;
                }

                var validationResult = PerformValidation();
                captureInfo.FinalCaptureSubdirectory = CaptureSubdirectory;

                return validationResult;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception calling PerformValidation";
                LogError("XMLTimeValidation.ValidateXMLFile(), Error calling PerformValidation", ex);
                return XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR;
            }
        }

        /// <summary>
        /// Determines if the size of a directory changes over specified time interval
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <param name="sleepIntervalSeconds"></param>
        /// <returns>True if constant, false if changed</returns>
        private bool VerifyConstantDirectorySize(string directoryPath, int sleepIntervalSeconds)
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
            var initialDirectorySize = mFileTools.GetDirectorySize(directoryPath);

            SleepWhileVerifyingConstantSize(sleepIntervalSeconds, "directory");

            // Get the final size of the directory and compare
            var finalDirectorySize = mFileTools.GetDirectorySize(directoryPath);

            return finalDirectorySize == initialDirectorySize;
        }

        /// <summary>
        /// Determines if the size of a file changes over specified time interval
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="sleepIntervalSeconds"></param>
        /// <param name="logonFailure"></param>
        /// <returns>True if constant, false if changed</returns>
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
                var datasetFile = new FileInfo(filePath);
                var initialFileSize = datasetFile.Length;

                SleepWhileVerifyingConstantSize(sleepIntervalSeconds, "file");

                // Get the final size of the file and compare
                datasetFile.Refresh();
                var finalFileSize = datasetFile.Length;

                return finalFileSize == initialFileSize;
            }
            catch (Exception ex)
            {
                LogWarning("Error accessing: " + filePath + ": " + ex.Message);

                // Check for "Logon failure: unknown username or bad password."

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
                    // ReSharper disable once GrammarMistakeInComment
                    // Note that error "The user name or password is incorrect" could be due to the Secondary Logon service not running
                    // We check for that in ProcessXmlTriggerFile.ProcessFile if ValidateXmlInfoMain returns false
                    throw;
                }
            }

            return false;
        }
    }
}
