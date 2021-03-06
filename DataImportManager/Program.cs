﻿using System;
using System.Threading;
using PRISM;
using PRISM.Logging;
using PRISM.FileProcessor;

namespace DataImportManager
{
    internal static class Program
    {
        public const string PROGRAM_DATE = "June 8, 2021";

        /// <summary>
        /// Entry method
        /// </summary>
        /// <returns>0 if no error, error code if an error</returns>
        public static int Main(string[] args)
        {
            try
            {
                var exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().GetName().Name);

                var cmdLineParser = new CommandLineParser<CommandLineOptions>(exeName,
                    ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE))
                {
                    ProgramInfo = "This program parses the instrument trigger files used for adding datasets to DMS. " +
                                  "Normal operation is to run the program without any command line switches.",
                    ContactInfo =
                        "Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                        Environment.NewLine +
                        "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                        "Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/" + Environment.NewLine + Environment.NewLine +
                        "Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License. " +
                        "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0"
                };

                var parsed = cmdLineParser.ParseArgs(args, false);
                var options = parsed.ParsedResults;
                if (args.Length > 0 && (!parsed.Success || !options.Validate()))
                {
                    // Delay for 1500 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                    Thread.Sleep(1500);
                    return -1;
                }

                if (options.TraceMode)
                {
                    ShowTraceMessage("Command line arguments parsed");
                }

                // Initiate automated analysis
                if (options.TraceMode)
                {
                    ShowTraceMessage("Instantiating clsMainProcess");
                }

                var mainProcess = new clsMainProcess(options.TraceMode)
                {
                    MailDisabled = options.NoMailMode,
                    PreviewMode = options.PreviewMode,
                    IgnoreInstrumentSourceErrors = options.IgnoreInstrumentSourceErrors
                };

                try
                {
                    if (!mainProcess.InitMgr())
                    {
                        if (options.TraceMode)
                        {
                            ShowTraceMessage("InitMgr returned false");
                        }

                        return -2;
                    }

                    if (options.TraceMode)
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

        private static void ShowErrorMessage(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowTraceMessage(string message)
        {
            clsMainProcess.ShowTraceMessage(message);
        }
    }
}
