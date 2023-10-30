using System;
using System.Collections.Concurrent;
using System.IO;
using PRISM.AppSettings;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class ProcessXmlTriggerFile : ProcessDatasetInfoBase
    {
        private FileInfo TriggerFile { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrSettings"></param>
        /// <param name="instrumentsToSkip"></param>
        /// <param name="infoCache"></param>
        /// <param name="udtSettings"></param>
        public ProcessXmlTriggerFile(
            MgrSettings mgrSettings,
            ConcurrentDictionary<string, int> instrumentsToSkip,
            DMSInfoCache infoCache,
            XmlProcSettingsType udtSettings) : base(mgrSettings, instrumentsToSkip, infoCache, udtSettings)
        {
        }

        protected override void FinalizeTask()
        {
            MoveXmlFile(TriggerFile, ProcSettings.SuccessDirectory);
            LogMessage("Completed data import task for dataset: " + TriggerFile.FullName);
        }

        /// <summary>
        /// Validate the XML trigger file, then send it to the database using mDataImportTask.PostTask
        /// </summary>
        /// <param name="triggerFile"></param>
        /// <returns>True if success, false if an error</returns>
        public bool ProcessFile(FileInfo triggerFile)
        {
            DatabaseErrorMsg = string.Empty;
            TriggerFile = triggerFile;

            var statusMsg = "Starting data import task for dataset: " + triggerFile.FullName;

            if (ProcSettings.TraceMode)
            {
                Console.WriteLine();
                Console.WriteLine("-------------------------------------------");
            }

            LogMessage(statusMsg);

            var triggerFileInfo = new TriggerFileInfo(triggerFile);

            return ProcessDatasetCaptureInfo(triggerFileInfo);
        }
    }
}
