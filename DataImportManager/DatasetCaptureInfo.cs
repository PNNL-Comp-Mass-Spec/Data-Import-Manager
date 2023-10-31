using System;

namespace DataImportManager
{
    internal abstract class DatasetCaptureInfo : LoggerBase
    {
        /// <summary>
        /// Dataset metadata names and IDs
        /// </summary>
        public enum DatasetMetadata
        {
            Dataset = 0,
            Experiment = 1,
            Instrument = 2,
            SeparationType = 3,
            LcCart = 4,
            LcCartConfig = 5,
            LcColumn = 6,
            Wellplate = 7,
            Well = 8,
            DatasetType = 9,
            OperatorUsername = 10,
            DsCreatorUsername = 11,
            Comment = 12,
            InterestRating = 13,
            Request = 14,
            WorkPackage = 15,
            EusUsageType = 16,
            EusProposalId = 17,
            EusUsers = 18,
            CaptureShareName = 19,
            CaptureSubdirectory = 20,
            RunStart = 21,  // Only tracked by XML trigger files, not by dataset creation task parameters
            RunFinish = 22  // Only tracked by XML trigger files, not by dataset creation task parameters
        }

        /// <summary>
        /// Capture share name
        /// </summary>
        public string CaptureShareName { get; set; } = string.Empty;

        /// <summary>
        /// Final capture subdirectory path
        /// </summary>
        public string FinalCaptureSubdirectory { get; set; } = string.Empty;

        /// <summary>
        /// True if <see cref="FinalCaptureSubdirectory"/> differs from <see cref="OriginalCaptureSubdirectory"/>
        /// </summary>
        public bool NeedsCaptureSubdirectoryReplacement => !OriginalCaptureSubdirectory.Equals(FinalCaptureSubdirectory, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Original capture subdirectory path
        /// </summary>
        public string OriginalCaptureSubdirectory { get; set; } = string.Empty;

        /// <summary>
        /// Either the filename of the source trigger file or the dataset creation queue task ID
        /// </summary>
        /// <param name="verbose">When true and processing an actual XML trigger file, this method will return the full path to the file; otherwise, returns the file name or a description of the dataset creation queue task</param>
        public abstract string GetSourceDescription(bool verbose = false);
    }
}
