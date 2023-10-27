using System;
using System.Data;
using System.Text.RegularExpressions;
using PRISM.AppSettings;
using PRISMDatabaseUtils;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class DataImportTask : DBTask
    {
        private string mPostTaskErrorMessage = string.Empty;
        private string mDatabaseErrorMessage;
        private string mStoredProc;

        public string PostTaskErrorMessage
        {
            get
            {
                if (string.IsNullOrEmpty(mPostTaskErrorMessage))
                {
                    return string.Empty;
                }

                return mPostTaskErrorMessage;
            }
        }

        public string DatabaseErrorMessage
        {
            get
            {
                if (string.IsNullOrEmpty(mDatabaseErrorMessage))
                {
                    return string.Empty;
                }

                return mDatabaseErrorMessage;
            }
        }

        /// <summary>
        /// When true, preview the datasets that would be added to DMS
        /// Also preview any e-mails that would be sent regarding errors
        /// </summary>
        public bool PreviewMode { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="dbTools"></param>
        public DataImportTask(MgrSettings mgrParams, IDBTools dbTools) : base(mgrParams, dbTools)
        {
        }

        /// <summary>
        /// Send XML trigger file data to the database to create a new dataset
        /// </summary>
        /// <param name="captureInfo">Dataset capture info</param>
        public bool PostTask(DatasetCaptureInfo captureInfo)
        {
            mPostTaskErrorMessage = string.Empty;
            mDatabaseErrorMessage = string.Empty;

            try
            {
                string triggerFileXML;

                if (captureInfo is TriggerFileInfo xmlTriggerFileInfo)
                {
                    // Load the XML file into memory
                    triggerFileXML = Global.LoadXmlFileContentsIntoString(xmlTriggerFileInfo.TriggerFile);
                }
                else if(captureInfo is DatasetCreateTaskInfo createTaskInfo)
                {
                    // Obtain the trigger file XML for the dataset creation task

                    if (!createTaskInfo.GetXmlTriggerFileParameters(out triggerFileXML))
                        return false;
                }
                else
                {
                    LogError("DatasetImportTask.PostTask(), captureInfo is not a recognized derived class");
                    return false;
                }

                if (string.IsNullOrEmpty(triggerFileXML))
                {
                    return false;
                }

                // Check and modify contents if needed. Also report any replacements to the log file.
                if (captureInfo.NeedsCaptureSubdirectoryReplacement)
                {
                    // <Parameter Name="Capture Subdirectory" Value="2020ESI_new.PRO\Data" />
                    var pattern = "(Parameter Name=\"Capture Sub(directory|folder)\" Value=\")" + Regex.Escape(captureInfo.OriginalCaptureSubdirectory) + "\"";
                    var updatedTriggerFileXML = Regex.Replace(triggerFileXML, pattern, $"$1{captureInfo.FinalCaptureSubdirectory}\"", RegexOptions.IgnoreCase);

                    LogMessage($"Replaced capture subdirectory \"{captureInfo.OriginalCaptureSubdirectory}\" with \"{captureInfo.FinalCaptureSubdirectory}\"", writeToLog: true);

                    // Call the stored procedure (typically add_new_dataset)
                    return ImportDataTask(updatedTriggerFileXML);
                }

                // Call the stored procedure (typically add_new_dataset)
                return ImportDataTask(triggerFileXML);
            }
            catch (Exception ex)
            {
                LogError("DatasetImportTask.PostTask(), Error running PostTask", ex);
                return false;
            }
        }

        /// <summary>
        /// Posts the given XML to DMS5 using stored procedure add_new_dataset
        /// </summary>
        /// <param name="triggerFileXML"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ImportDataTask(string triggerFileXML)
        {
            try
            {
                // Initialize database error message
                mDatabaseErrorMessage = string.Empty;

                // Prepare to call the stored procedure, typically named add_new_dataset in DMS5, which in turn calls add_update_dataset
                // (old procedure names: AddNewDataset and AddUpdateDataset)
                mStoredProc = MgrParams.GetParam("StoredProcedure");

                var cmd = DBTools.CreateCommand(mStoredProc, CommandType.StoredProcedure);
                cmd.CommandTimeout = 45;

                // Define parameter for procedure's return value
                // If querying a Postgres DB, DBTools will auto-change "@return" to "_returnCode"
                var returnParam = DBTools.AddParameter(cmd, "@Return", SqlType.Int, direction: ParameterDirection.ReturnValue);

                DBTools.AddParameter(cmd, "@XmlDoc", SqlType.VarChar, 4000, triggerFileXML);
                DBTools.AddParameter(cmd, "@mode", SqlType.VarChar, 24, "add");
                var messageParam = DBTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, direction: ParameterDirection.InputOutput);

                if (PreviewMode)
                {
                    MainProcess.ShowTraceMessage("Preview: call stored procedure " + mStoredProc + " in database " + DBTools.DatabaseName);
                    return true;
                }

                if (TraceMode)
                {
                    MainProcess.ShowTraceMessage("Calling stored procedure " + mStoredProc + " in database " + DBTools.DatabaseName);
                }

                // Execute the stored procedure
                DBTools.ExecuteSP(cmd);

                // Get return code
                var returnCode = DBToolsBase.GetReturnCode(returnParam);

                if (returnCode == 0)
                {
                    // Get values for output parameters
                    return true;
                }

                mPostTaskErrorMessage = messageParam.Value.CastDBVal<string>();

                LogError(string.Format("DataImportTask.ImportDataTask(), Problem posting dataset (return code {0}): {1}",
                    returnParam.Value.CastDBVal<string>(), mPostTaskErrorMessage));

                return false;
            }
            catch (Exception ex)
            {
                LogError("DataImportTask.ImportDataTask(), Error posting dataset", ex, true);
                mDatabaseErrorMessage = Environment.NewLine + ("Database Error Message:" + ex.Message);
                return false;
            }
        }
    }
}
