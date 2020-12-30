
namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class clsValidationError
    {
        public string IssueType { get; }

        public string IssueDetail { get; }

        public string AdditionalInfo { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsValidationError(string issueType, string issueDetail)
        {
            IssueType = issueType;
            IssueDetail = issueDetail;
            AdditionalInfo = string.Empty;
        }
    }
}
