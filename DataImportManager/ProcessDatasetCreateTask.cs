using PRISM.AppSettings;
using System;
using System.Collections.Concurrent;

namespace DataImportManager
{
    internal class ProcessDatasetCreateTask : ProcessDatasetInfoBase
    {
        private int TaskID { get; set; } = 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrSettings"></param>
        /// <param name="instrumentsToSkip"></param>
        /// <param name="infoCache"></param>
        /// <param name="udtSettings"></param>
        public ProcessDatasetCreateTask(
            MgrSettings mgrSettings,
            ConcurrentDictionary<string, int> instrumentsToSkip,
            DMSInfoCache infoCache,
            XmlProcSettingsType udtSettings) : base(mgrSettings, instrumentsToSkip, infoCache, udtSettings)
        {
        }

        protected override void FinalizeTask()
        {
            LogMessage("Completed data import task for dataset creation queue item " + TaskID);
        }

        /// <summary>
        /// Validate the XML parameters, then send them to the database using mDataImportTask.PostTask
        /// </summary>
        /// <param name="taskID">ID of the row in t_dataset_create_queue</param>
        /// <param name="xmlParameters"></param>
        /// <returns>True if success, false if an error</returns>
        public bool ProcessXmlParameters(int taskID, string xmlParameters)
        {
            DatabaseErrorMsg = string.Empty;
            TaskID = taskID;

            var statusMsg = string.Format("Starting data import task for dataset creation task ID {0}", taskID);

            if (ProcSettings.TraceMode)
            {
                Console.WriteLine();
                Console.WriteLine("-------------------------------------------");
            }

            LogMessage(statusMsg);

            var triggerFileInfo = new DatasetCreateTaskInfo(taskID, xmlParameters);

            return ProcessDatasetCaptureInfo(triggerFileInfo);
        }
    }
}
