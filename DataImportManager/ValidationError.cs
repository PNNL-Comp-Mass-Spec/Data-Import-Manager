
namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class ValidationError
    {
        public string IssueType { get; }

        public string IssueDetail { get; }

        public string AdditionalInfo { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public ValidationError(string issueType, string issueDetail)
        {
            IssueType = issueType;
            IssueDetail = issueDetail;
            AdditionalInfo = string.Empty;
        }
    }
}
