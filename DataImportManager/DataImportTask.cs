using System;
using System.Data;
using System.Text.RegularExpressions;
using System.Threading;
using PRISM.AppSettings;
using PRISMDatabaseUtils;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class DataImportTask : DBTask
    {
        private string mPostTaskErrorMessage = string.Empty;
        private string mDataImportErrorMessage;
        private string mDataImportErrorMessageForDatabase;
        private string mStoredProc;

        private static readonly SemaphoreSlim dbCallLimiter;
        private const int MAX_PARALLEL_DB_CALLS = 6;

        static DataImportTask()
        {
            // Only allow up to 6 database calls in parallel
            dbCallLimiter = new SemaphoreSlim(MAX_PARALLEL_DB_CALLS, MAX_PARALLEL_DB_CALLS);
        }

        public static void DisposeSemaphore()
        {
            dbCallLimiter.Dispose();
        }

        /// <summary>
        /// Error message returned by procedure add_new_dataset (which in turn calls add_update_dataset)
        /// </summary>
        public string PostTaskErrorMessage
        {
            get
            {
                if (string.IsNullOrWhiteSpace(mPostTaskErrorMessage))
                {
                    return string.Empty;
                }

                return mPostTaskErrorMessage;
            }
        }

        /// <summary>
        /// Data import error message to mail to the user
        /// </summary>
        public string DataImportErrorMessage
        {
            get
            {
                if (string.IsNullOrWhiteSpace(mDataImportErrorMessage))
                {
                    return string.Empty;
                }

                return mDataImportErrorMessage;
            }
        }

        /// <summary>
        /// Data import error message to store in T_Dataset_Create_Queue
        /// </summary>
        /// <remarks>This is the error message, but without any suggested fixes</remarks>
        public string DataImportErrorMessageForDatabase
        {
            get
            {
                if (string.IsNullOrWhiteSpace(mDataImportErrorMessageForDatabase))
                {
                    return string.Empty;
                }

                return mDataImportErrorMessageForDatabase;
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
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="dbTools">DBTools instance</param>
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
            mDataImportErrorMessage = string.Empty;
            mDataImportErrorMessageForDatabase = string.Empty;

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

                if (string.IsNullOrWhiteSpace(triggerFileXML))
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

                    // Call the procedure (typically add_new_dataset)
                    return ImportDataTaskLimited(updatedTriggerFileXML);
                }

                // Call the procedure (typically add_new_dataset)
                return ImportDataTaskLimited(triggerFileXML);
            }
            catch (Exception ex)
            {
                LogError("DatasetImportTask.PostTask(), Error running PostTask", ex);
                return false;
            }
        }

        /// <summary>
        /// Posts the given XML to DMS using procedure add_new_dataset, using a semaphore to restrict parallel calls to a certain count
        /// </summary>
        /// <param name="triggerFileXML">Dataset metadata XML</param>
        /// <returns>True if success, false if an error</returns>
        private bool ImportDataTaskLimited(string triggerFileXML)
        {
            dbCallLimiter.Wait();
            try
            {
                return ImportDataTask(triggerFileXML);
            }
            finally
            {
                dbCallLimiter.Release();
            }
        }

        /// <summary>
        /// Posts the given XML to DMS using procedure add_new_dataset
        /// </summary>
        /// <param name="triggerFileXML">Dataset metadata XML</param>
        /// <returns>True if success, false if an error</returns>
        private bool ImportDataTask(string triggerFileXML)
        {
            try
            {
                mDataImportErrorMessage = string.Empty;
                mDataImportErrorMessageForDatabase = string.Empty;

                // Prepare to call the procedure, typically named add_new_dataset in DMS, which in turn calls add_update_dataset
                // (old procedure names: AddNewDataset and AddUpdateDataset)
                mStoredProc = MgrParams.GetParam("StoredProcedure");

                var cmd = DBTools.CreateCommand(mStoredProc, CommandType.StoredProcedure);
                cmd.CommandTimeout = 45;

                DBTools.AddParameter(cmd, "@xmlDoc", SqlType.VarChar, 4000, triggerFileXML);
                DBTools.AddParameter(cmd, "@mode", SqlType.VarChar, 24, "add");
                var messageParam = DBTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

                // Define parameter for procedure's return value
                // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
                var returnParam = DBTools.AddParameter(cmd, "@return", SqlType.Int, ParameterDirection.ReturnValue);

                if (PreviewMode)
                {
                    MainProcess.ShowTraceMessage("Preview: call procedure " + mStoredProc + " in database " + DBTools.DatabaseName);
                    return true;
                }

                if (TraceMode)
                {
                    MainProcess.ShowTraceMessage("Calling procedure " + mStoredProc + " in database " + DBTools.DatabaseName);
                }

                // Call the procedure
                var resultCode = DBTools.ExecuteSP(cmd, out var errorMessage);

                // resultCode will always have a value, even on failure due to errors outside the procedure.
                // if resultCode is less than zero, it is always an error
                if (resultCode >= 0 && string.IsNullOrWhiteSpace(errorMessage))
                {
                    // Get return code
                    var returnCode = DBToolsBase.GetReturnCode(returnParam);

                    if (returnCode == 0)
                    {
                        // Get values for output parameters
                        return true;
                    }
                }

                mPostTaskErrorMessage = messageParam.Value.CastDBVal<string>();
                if (string.IsNullOrWhiteSpace(mPostTaskErrorMessage) && !string.IsNullOrWhiteSpace(errorMessage))
                {
                    mPostTaskErrorMessage = errorMessage;
                }

                LogError(string.Format("DataImportTask.ImportDataTask(), Problem posting dataset (return code {0}): {1}",
                    returnParam.Value.CastDBVal<string>(), mPostTaskErrorMessage));

                return false;
            }
            catch (Exception ex)
            {
                LogError("DataImportTask.ImportDataTask(), Error posting dataset", ex, true);

                mDataImportErrorMessage = Environment.NewLine + "Database Error Message: " + ex.Message;
                mDataImportErrorMessageForDatabase = "Exception in ImportDataTask: " + ex.Message;

                return false;
            }
        }
    }
}
