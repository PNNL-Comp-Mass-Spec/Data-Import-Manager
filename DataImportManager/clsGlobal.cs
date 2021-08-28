using System;
using System.Globalization;
using System.IO;
using System.Text;
using PRISM.Logging;

namespace DataImportManager
{
    /// <summary>
    /// Global methods used by other classes
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public static class clsGlobal
    {
        //  Constants
        private const string FLAG_FILE_NAME = "FlagFile.txt";

        /// <summary>
        /// Creates a dummy file in the application directory to be used for controlling task request bypass
        /// </summary>
        public static void CreateStatusFlagFile()
        {
            var exeDirectoryPath = GetExeDirectoryPath();
            var flagFile = new FileInfo(Path.Combine(exeDirectoryPath, FLAG_FILE_NAME));

            using var writer = flagFile.AppendText();
            writer.WriteLine(DateTime.Now.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Deletes the flag file
        /// </summary>
        public static void DeleteStatusFlagFile()
        {
            var exeDirectoryPath = GetExeDirectoryPath();
            var flagFilePath = Path.Combine(exeDirectoryPath, FLAG_FILE_NAME);

            try
            {
                if (File.Exists(flagFilePath))
                {
                    File.Delete(flagFilePath);
                }
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error in DeleteStatusFlagFile", ex);
            }
        }

        /// <summary>
        /// Looks for the flag file
        /// </summary>
        /// <returns>True if flag file exists</returns>
        public static bool DetectStatusFlagFile()
        {
            var exeDirectoryPath = GetExeDirectoryPath();
            var flagFilePath = Path.Combine(exeDirectoryPath, FLAG_FILE_NAME);
            return File.Exists(flagFilePath);
        }

        /// <summary>
        /// Returns the full path to the directory with the executable
        /// </summary>
        /// <remarks>Returns an empty string if unable to determine the parent directory</remarks>
        public static string GetExeDirectoryPath()
        {
            var exeFile = new FileInfo(GetExePath());
            if (exeFile.Directory == null)
                return "";

            return exeFile.DirectoryName;
        }

        /// <summary>
        /// Returns the full path to this application's executable
        /// </summary>
        public static string GetExePath()
        {
            return PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath();
        }

        /// <summary>
        /// Current host name
        /// </summary>
        public static string GetHostName()
        {
            var hostName = System.Net.Dns.GetHostName();
            return string.IsNullOrWhiteSpace(hostName) ? Environment.MachineName : hostName;
        }

        /// <summary>
        /// Load an XML file into memory and return as a string
        /// </summary>
        /// <remarks>Replaces the ampersand character with &#38;</remarks>
        /// <param name="xmlFile"></param>
        public static string LoadXmlFileContentsIntoString(FileInfo xmlFile)
        {
            try
            {
                if (!xmlFile.Exists)
                {
                    LogTools.LogError("clsGlobal.LoadXmlFileContentsIntoString(), File: " + xmlFile.FullName + " does not exist.");
                    return string.Empty;
                }

                var xmlFileContents = new StringBuilder();

                using var reader = new StreamReader(new FileStream(xmlFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var input = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(input))
                        continue;

                    if (xmlFileContents.Length > 0)
                    {
                        xmlFileContents.Append(Environment.NewLine);
                    }

                    xmlFileContents.Append(input.Replace("&", "&#38;"));
                }

                return xmlFileContents.ToString();
            }
            catch (Exception ex)
            {
                LogTools.LogError("clsGlobal.LoadXmlFileContentsIntoString(), Error reading xml file", ex);
                return string.Empty;
            }
        }
    }
}
