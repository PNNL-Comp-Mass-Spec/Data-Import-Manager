﻿using System.Collections.Generic;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class ValidationErrorSummary
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
        /// <param name="issueType">Issue type</param>
        /// <param name="sortWeight">Sort weight</param>
        public ValidationErrorSummary(string issueType, int sortWeight)
        {
            IssueType = issueType;
            SortWeight = sortWeight;

            AffectedItems = new List<AffectedItemType>();
            DatabaseErrorMsg = string.Empty;
        }
    }
}
