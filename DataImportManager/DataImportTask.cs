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
        private string mXmlContents;

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
        /// Send the contents of an XML trigger file to the database to create a new dataset
        /// </summary>
        /// <param name="triggerFileInfo"></param>
        public bool PostTask(TriggerFileInfo triggerFileInfo)
        {
            mPostTaskErrorMessage = string.Empty;
            mDatabaseErrorMessage = string.Empty;

            bool fileImported;

            try
            {
                // Load the XML file into memory
                mXmlContents = Global.LoadXmlFileContentsIntoString(triggerFileInfo.TriggerFile);

                if (string.IsNullOrEmpty(mXmlContents))
                {
                    return false;
                }

                // Check and modify contents if needed. Also report any replacements to the log file.
                if (triggerFileInfo.NeedsCaptureSubdirectoryReplacement)
                {
                    // <Parameter Name="Capture Subdirectory" Value="2020ESI_new.PRO\Data" />
                    var pattern = "(Parameter Name=\"Capture Sub(directory|folder)\" Value=\")" + Regex.Escape(triggerFileInfo.OriginalCaptureSubdirectory) + "\"";
                    mXmlContents = Regex.Replace(mXmlContents, pattern, $"$1{triggerFileInfo.FinalCaptureSubdirectory}\"", RegexOptions.IgnoreCase);
                    LogMessage($"Replaced capture subdirectory \"{triggerFileInfo.OriginalCaptureSubdirectory}\" with \"{triggerFileInfo.FinalCaptureSubdirectory}\"", writeToLog: true);
                }

                // Call the stored procedure (typically add_new_dataset)
                fileImported = ImportDataTask();
            }
            catch (Exception ex)
            {
                LogError("DatasetImportTask.PostTask(), Error running PostTask", ex);
                return false;
            }

            return fileImported;
        }

        /// <summary>
        /// Posts the given XML to DMS5 using stored procedure add_new_dataset
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool ImportDataTask()
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

                // Define parameter for stored procedure's return value
                var returnParam = DBTools.AddParameter(cmd, "@Return", SqlType.Int, direction: ParameterDirection.ReturnValue);
                DBTools.AddParameter(cmd, "@XmlDoc", SqlType.VarChar, 4000, mXmlContents);
                DBTools.AddParameter(cmd, "@mode", SqlType.VarChar, 24, "add");
                var messageParam = DBTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, direction: ParameterDirection.Output);

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
                var returnCode = DBTools.ExecuteSP(cmd);

                // Get return code
                var returnParamText = DBTools.GetString(returnParam.Value);

                if (returnCode == 0)
                {
                    // Get values for output parameters
                    return true;
                }

                mPostTaskErrorMessage = DBTools.GetString(messageParam.Value) ?? string.Empty;
                LogError(string.Format("DataImportTask.ImportDataTask(), Problem posting dataset (return code {0}): {1}",
                    returnParamText ?? string.Empty, mPostTaskErrorMessage));

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
