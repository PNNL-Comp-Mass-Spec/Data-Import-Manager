using System.Data.SqlClient;
using PRISM.AppSettings;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal abstract class clsDBTask : clsLoggerBase
    {
        /// <summary>
        /// Manager parameters
        /// </summary>
        protected MgrSettings MgrParams { get; }

        /// <summary>
        /// Database connection object
        /// </summary>
        protected SqlConnection DatabaseConnection { get; }

        /// <summary>
        /// When true, show additional debug messages as the console
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="dbConnection">Database connection object (connection should already be open)</param>
        protected clsDBTask(MgrSettings mgrParams, SqlConnection dbConnection)
        {
            MgrParams = mgrParams;
            DatabaseConnection = dbConnection;
        }
    }
}
