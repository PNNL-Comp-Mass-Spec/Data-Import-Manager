using System.Collections.Generic;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class clsQueuedMail
    {

        #region "Properties"

        public string InstrumentOperator { get; }

        /// <summary>
        /// Semi-colon separated list of e-mail addresses
        /// </summary>
        /// <remarks></remarks>
        public string Recipients { get; }

        public string Subject { get; }

        /// <summary>
        /// Tracks any database message errors
        /// Also used to track suggested solutions
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string DatabaseErrorMsg { get; set; }

        public List<clsValidationError> ValidationErrors { get; }

        /// <summary>
        /// Tracks the path to the dataset on the instrument
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string InstrumentDatasetPath { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="operatorName"></param>
        /// <param name="recipientList"></param>
        /// <param name="mailSubject"></param>
        /// <param name="lstValidationErrors"></param>
        public clsQueuedMail(string operatorName, string recipientList, string mailSubject, List<clsValidationError> lstValidationErrors)
        {
            InstrumentOperator = operatorName;
            Recipients = recipientList;
            Subject = mailSubject;
            ValidationErrors = lstValidationErrors;

            DatabaseErrorMsg = string.Empty;
            InstrumentDatasetPath = string.Empty;
        }
    }
}
