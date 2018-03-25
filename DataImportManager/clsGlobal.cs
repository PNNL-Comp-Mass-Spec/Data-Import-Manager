using System;
using System.Globalization;
using System.IO;
using System.Text;
using PRISM;
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
            var fiFlagFile = new FileInfo(Path.Combine(exeDirectoryPath, FLAG_FILE_NAME));
            using (var swFlagFile = fiFlagFile.AppendText())
            {
                swFlagFile.WriteLine(DateTime.Now.ToString(CultureInfo.InvariantCulture));
            }
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
        /// Show an error at the console when unable to write to the log file
        /// </summary>
        /// <param name="logMessage"></param>
        /// <param name="ex"></param>
        public static void ErrorWritingToLog(string logMessage, Exception ex)
        {
            ConsoleMsgUtils.ShowError("Error logging errors; log message: " + logMessage, ex);
        }

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex)
        {
            return GetExceptionStackTrace(ex, false);
        }

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="multiLineOutput">When true, format the stack trace using newline characters instead of -:-</param>
        /// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex, bool multiLineOutput)
        {
            if (multiLineOutput)
            {
                return clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex);
            }

            return clsStackTraceFormatter.GetExceptionStackTrace(ex);
        }

        /// <summary>
        /// Returns the full path to the directory with the executable
        /// </summary>
        /// <returns></returns>
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
        /// <returns></returns>
        public static string GetExePath()
        {
            return PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath();
        }

        /// <summary>
        /// Current host name
        /// </summary>
        /// <returns></returns>
        public static string GetHostName()
        {
            var hostName = System.Net.Dns.GetHostName();
            if (string.IsNullOrWhiteSpace(hostName))
            {
                hostName = Environment.MachineName;
            }

            return hostName;
        }

        /// <summary>
        /// Load an XML file into mememory and return as a string
        /// </summary>
        /// <param name="xmlFile"></param>
        /// <returns></returns>
        /// <remarks>Replaces the ambersand character with &#38;</remarks>
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
                using (var sr = new StreamReader(new FileStream(xmlFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!sr.EndOfStream)
                    {
                        var input = sr.ReadLine();
                        if (string.IsNullOrWhiteSpace(input))
                            continue;

                        if (xmlFileContents.Length > 0)
                        {
                            xmlFileContents.Append(Environment.NewLine);
                        }

                        xmlFileContents.Append(input.Replace("&", "&#38;"));
                    }
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