using System.Collections.Generic;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class clsValidationErrorSummary
    {
        public struct AffectedItemType
        {
            public string IssueDetail;
            public string AdditionalInfo;
        }

        public List<AffectedItemType> AffectedItems { get; }

        public string DatabaseErrorMsg { get; set;  }

        public string IssueType { get; }

        public int SortWeight { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="issueType"></param>
        /// <param name="sortWeight"></param>
        public clsValidationErrorSummary(string issueType, int sortWeight)
        {
            IssueType = issueType;
            SortWeight = sortWeight;

            AffectedItems = new List<AffectedItemType>();
            DatabaseErrorMsg = string.Empty;
        }
    }
}
