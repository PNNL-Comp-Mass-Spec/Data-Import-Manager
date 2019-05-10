using System;
using System.Collections.Generic;
using System.Linq;
using PRISM;
using PRISM.Logging;
using PRISM.FileProcessor;

namespace DataImportManager
{
    internal class Program
    {
        public const string PROGRAM_DATE = "May 9, 2019";

        private static bool mMailDisabled;

        private static bool mTraceMode;

        private static bool mPreviewMode;

        private static bool mIgnoreInstrumentSourceErrors;

        /// <summary>
        /// Entry method
        /// </summary>
        /// <returns>0 if no error, error code if an error</returns>
        public static int Main(string[] args)
        {
            var commandLineParser = new clsParseCommandLine();

            mMailDisabled = false;
            mTraceMode = false;
            mPreviewMode = false;
            mIgnoreInstrumentSourceErrors = false;

            try
            {
                bool validArgs;

                // Parse the command line options
                if (commandLineParser.ParseCommandLine())
                {
                    validArgs = SetOptionsUsingCommandLineParameters(commandLineParser);
                }
                else if (commandLineParser.NoParameters)
                {
                    validArgs = true;
                }
                else
                {
                    if (commandLineParser.NeedToShowHelp)
                    {
                        ShowProgramHelp();
                    }
                    else
                    {
                        ConsoleMsgUtils.ShowWarning("Error parsing the command line arguments");
                        clsParseCommandLine.PauseAtConsole(750);
                    }

                    return -1;
                }

                if (commandLineParser.NeedToShowHelp || !validArgs)
                {
                    ShowProgramHelp();
                    return -1;
                }

                if (mTraceMode)
                {
                    ShowTraceMessage("Command line arguments parsed");
                }

                // Initiate automated analysis
                if (mTraceMode)
                {
                    ShowTraceMessage("Instantiating clsMainProcess");
                }

                var mainProcess = new clsMainProcess(mTraceMode)
                {
                    MailDisabled = mMailDisabled,
                    PreviewMode = mPreviewMode,
                    IgnoreInstrumentSourceErrors = mIgnoreInstrumentSourceErrors
                };

                try
                {
                    if (!mainProcess.InitMgr())
                    {
                        if (mTraceMode)
                        {
                            ShowTraceMessage("InitMgr returned false");
                        }

                        return -2;
                    }

                    if (mTraceMode)
                    {
                        ShowTraceMessage("Manager initialized");
                    }

                }
                catch (Exception ex)
                {
                    ShowErrorMessage("Exception thrown by InitMgr: ", ex);

                    LogTools.FlushPendingMessages();
                    return -1;
                }

                mainProcess.DoImport();
                LogTools.FlushPendingMessages();
                return 0;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error occurred in modMain->Main", ex);
                LogTools.FlushPendingMessages();
                return -1;
            }

        }


        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false
            var validParameters = new List<string>
            {
                "NoMail",
                "Trace",
                "Preview",
                "ISE"
            };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(validParameters))
                {
                    ShowErrorMessage("Invalid command line parameters",
                        (from item in commandLineParser.InvalidParameters(validParameters) select "/" + item).ToList());

                    return false;
                }

                // Query commandLineParser to see if various parameters are present
                if (commandLineParser.IsParameterPresent("NoMail"))
                {
                    mMailDisabled = true;
                }

                if (commandLineParser.IsParameterPresent("Trace"))
                {
                    mTraceMode = true;
                }

                if (commandLineParser.IsParameterPresent("Preview"))
                {
                    mPreviewMode = true;
                }

                if (commandLineParser.IsParameterPresent("ISE"))
                {
                    mIgnoreInstrumentSourceErrors = true;
                }

                if (mPreviewMode)
                {
                    mMailDisabled = true;
                    mTraceMode = true;
                }

                return true;

            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters: ", ex);
            }

            return false;
        }

        private static void ShowErrorMessage(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowErrorMessage(string title, IEnumerable<string> errorMessages)
        {
            ConsoleMsgUtils.ShowErrors(title, errorMessages);
        }

        private static void ShowProgramHelp()
        {
            try
            {
                var exePath = clsGlobal.GetExePath();

                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "This program parses the instrument trigger files used for adding datasets to DMS. " +
                    "Normal operation is to run the program without any command line switches."));
                Console.WriteLine();
                Console.WriteLine("Program syntax:");
                Console.WriteLine(exePath + " [/NoMail] [/Trace] [/Preview] [/ISE]");
                Console.WriteLine();
                Console.WriteLine("Use /NoMail to disable sending e-mail when errors are encountered");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "Use /Trace to enable trace mode, where debug messages are written to the command prompt"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "Use /Preview to enable preview mode, where we report any trigger files found, " +
                    "but do not post them to DMS and do not move them to the failure directory if there is an error. " +
                    "Using /Preview forces /NoMail and /Trace to both be enabled"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "Use /ISE to ignore instrument source check errors (e.g. cannot access bionet)"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)"));
                Console.WriteLine();
                Console.WriteLine("Version: " + ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE));
                Console.WriteLine();
                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License. " +
                    "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0"));
                Console.WriteLine();

                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                clsParseCommandLine.PauseAtConsole(750);
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error displaying the program syntax", ex);
            }

        }

        private static void ShowTraceMessage(string message)
        {
            clsMainProcess.ShowTraceMessage(message);
        }
    }
}
