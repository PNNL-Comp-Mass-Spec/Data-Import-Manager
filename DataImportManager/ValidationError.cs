
namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class ValidationError
    {
        /// <summary>
        /// Issue type
        /// </summary>
        public string IssueType { get; }

        /// <summary>
        /// Issue detail
        /// </summary>
        public string IssueDetail { get; }

        /// <summary>
        /// Additional info
        /// </summary>
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
