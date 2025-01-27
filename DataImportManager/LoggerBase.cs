﻿using System;
using PRISM;
using PRISM.Logging;

namespace DataImportManager
{
    /// <summary>
    /// Class with logging methods that call PRISM.Logging.LogTools
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public abstract class LoggerBase
    {
        /// <summary>
        /// Show a status message at the console and optionally include in the log file, tagging it as a debug message
        /// </summary>
        /// <remarks>The message is shown in dark gray in the console.</remarks>
        /// <param name="statusMessage">Status message</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        protected static void LogDebug(string statusMessage, bool writeToLog = true)
        {
            LogTools.LogDebug(statusMessage, writeToLog);
        }

        /// <summary>
        /// Log an error message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected static void LogError(string errorMessage, bool logToDb = false)
        {
            LogTools.LogError(errorMessage, null, logToDb);
        }

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception to log</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected static void LogError(string errorMessage, Exception ex, bool logToDb = false)
        {
            LogTools.LogError(errorMessage, ex, logToDb);
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="isError">True if this is an error</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        public static void LogMessage(string statusMessage, bool isError = false, bool writeToLog = true)
        {
            LogTools.LogMessage(statusMessage, isError, writeToLog);
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected static void LogWarning(string warningMessage, bool logToDb = false)
        {
            LogTools.LogWarning(warningMessage, logToDb);
        }

        /// <summary>
        /// Register event handlers
        /// However, does not subscribe to .ProgressUpdate
        /// Note: the DatasetInfoPlugin does subscribe to .ProgressUpdate
        /// </summary>
        /// <param name="processingClass">Processing class</param>
        /// <param name="writeDebugEventsToLog">When true, log debug events to the log</param>
        protected void RegisterEvents(IEventNotifier processingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                processingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                processingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            processingClass.StatusEvent += StatusEventHandler;
            processingClass.ErrorEvent += ErrorEventHandler;
            processingClass.WarningEvent += WarningEventHandler;
            // Ignore: processingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        /// <summary>
        /// Unregister the event handler for the given LogLevel
        /// </summary>
        /// <param name="processingClass">Processing class</param>
        /// <param name="messageType">Message type</param>
        protected void UnregisterEventHandler(EventNotifier processingClass, BaseLogger.LogLevels messageType)
        {
            switch (messageType)
            {
                case BaseLogger.LogLevels.DEBUG:
                    processingClass.DebugEvent -= DebugEventHandler;
                    processingClass.DebugEvent -= DebugEventHandlerConsoleOnly;
                    break;
                case BaseLogger.LogLevels.ERROR:
                    processingClass.ErrorEvent -= ErrorEventHandler;
                    break;
                case BaseLogger.LogLevels.WARN:
                    processingClass.WarningEvent -= WarningEventHandler;
                    break;
                case BaseLogger.LogLevels.INFO:
                    processingClass.StatusEvent -= StatusEventHandler;
                    break;
                default:
                    throw new Exception("Log level not supported for unregistering");
            }
        }

        protected void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogDebug(statusMessage, writeToLog: false);
        }

        protected void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        protected void StatusEventHandler(string statusMessage)
        {
            LogMessage(statusMessage);
        }

        protected void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        protected void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }
    }
}
