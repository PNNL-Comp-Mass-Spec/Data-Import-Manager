using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class clsDataImportTask : clsDBTask
    {
        #region "Member Variables"

        private string mPostTaskErrorMessage = string.Empty;
        private string mDatabaseErrorMessage;
        private string mStoredProc;
        private string mXmlContents;

        #endregion

        #region "Properties"

        public string PostTaskErrorMessage
        {
            get
            {
                if (string.IsNullOrEmpty(mPostTaskErrorMessage))
                {
                    return string.Empty;
                }
                else
                {
                    return mPostTaskErrorMessage;
                }
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

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="dbConnection"></param>
        public clsDataImportTask(clsMgrSettings mgrParams, SqlConnection dbConnection) : base(mgrParams, dbConnection)
        {
        }

        /// <summary>
        /// Send the contents of an XML trigger file to the database to create a new dataset
        /// </summary>
        /// <param name="triggerFile"></param>
        /// <returns></returns>
        public bool PostTask(FileInfo triggerFile)
        {
            mPostTaskErrorMessage = string.Empty;
            mDatabaseErrorMessage = string.Empty;

            bool fileImported;
            try
            {
                // Load the XML file into memory
                mXmlContents = clsGlobal.LoadXmlFileContentsIntoString(triggerFile);
                if (string.IsNullOrEmpty(mXmlContents))
                {
                    return false;
                }

                // Call the stored procedure (typically AddNewDataset)
                fileImported = ImportDataTask();
            }
            catch (Exception ex)
            {
                LogError("clsDatasetImportTask.PostTask(), Error running PostTask", ex);
                return false;
            }

            return fileImported;
        }

        /// <summary>
        /// Posts the given XML to DMS5 using stored procedure AddNewDataset
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool ImportDataTask()
        {
            try
            {
                // Initialize database error message
                mDatabaseErrorMessage = string.Empty;

                // Prepare to call the stored procedure (typically AddNewDataset in DMS5, which in turn calls AddUpdateDataset)
                mStoredProc = MgrParams.GetParam("storedprocedure");

                var spCmd = new SqlCommand(mStoredProc, DatabaseConnection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 45
                };

                // Define parameter for stored procedure's return value
                spCmd.Parameters.Add("@Return", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                spCmd.Parameters.Add("@XmlDoc", SqlDbType.VarChar, 4000).Value = mXmlContents;
                spCmd.Parameters.Add("@mode", SqlDbType.VarChar, 24).Value = "add";
                spCmd.Parameters.Add("@message", SqlDbType.VarChar, 512).Direction = ParameterDirection.Output;

                if (PreviewMode)
                {
                    clsMainProcess.ShowTraceMessage("Preview: call stored procedure " + mStoredProc + " in database " + DatabaseConnection.Database);
                    return true;
                }

                if (TraceMode)
                {
                    clsMainProcess.ShowTraceMessage("Calling stored procedure " + mStoredProc + " in database " + DatabaseConnection.Database);
                }

                // Execute the stored procedure
                spCmd.ExecuteNonQuery();

                // Get return value
                var ret = Convert.ToInt32(spCmd.Parameters["@Return"].Value);
                if (ret == 0)
                {
                    // Get values for output parameters
                    return true;
                }

                mPostTaskErrorMessage = spCmd.Parameters["@message"].Value.ToString();
                LogError("clsDataImportTask.ImportDataTask(), Problem posting dataset: " + mPostTaskErrorMessage);
                return false;

            }
            catch (Exception ex)
            {
                LogError("clsDataImportTask.ImportDataTask(), Error posting dataset", ex, true);
                mDatabaseErrorMessage = Environment.NewLine + ("Database Error Message:" + ex.Message);
                return false;
            }

        }
    }
}
