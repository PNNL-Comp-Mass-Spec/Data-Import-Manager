using PRISM.AppSettings;
using PRISMDatabaseUtils;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal abstract class DBTask : LoggerBase
    {
        /// <summary>
        /// Manager parameters
        /// </summary>
        protected MgrSettings MgrParams { get; }

        /// <summary>
        /// Database tools object
        /// </summary>
        protected IDBTools DBTools { get; }

        /// <summary>
        /// When true, show additional debug messages as the console
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="dbTools">Database tools object</param>
        protected DBTask(MgrSettings mgrParams, IDBTools dbTools)
        {
            MgrParams = mgrParams;
            DBTools = dbTools;
        }
    }
}
