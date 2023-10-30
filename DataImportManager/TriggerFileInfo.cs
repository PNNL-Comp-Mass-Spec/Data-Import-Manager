using System.Collections.Generic;
using System.IO;

namespace DataImportManager
{
    internal class TriggerFileInfo : DatasetCaptureInfo
    {
        /// <summary>
        /// XML trigger file
        /// </summary>
        public FileInfo TriggerFile { get; }

        /// <summary>
        /// Dictionary mapping dataset metadata enum values to the parameter names in the XML loaded from an XML trigger file
        /// </summary>
        public Dictionary<DatasetMetadata, string> TriggerFileParamNames { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public TriggerFileInfo(FileInfo triggerFile)
        {
            TriggerFile = triggerFile;
            TriggerFileParamNames = GetTriggerFileParamNameMap();
        }

        public static Dictionary<DatasetMetadata, string> GetTriggerFileParamNameMap()
        {
            return new Dictionary<DatasetMetadata, string>
            {
                { DatasetMetadata.Dataset, "Dataset Name" },
                { DatasetMetadata.Experiment, "Experiment Name" },
                { DatasetMetadata.Instrument, "Instrument Name" },
                { DatasetMetadata.SeparationType, "Separation Type" },
                { DatasetMetadata.LcCart, "LC Cart Name" },
                { DatasetMetadata.LcCartConfig, "LC Cart Config" },
                { DatasetMetadata.LcColumn, "LC Column" },
                { DatasetMetadata.Wellplate, string.Empty },
                { DatasetMetadata.Well, string.Empty },
                { DatasetMetadata.DatasetType, "Dataset Type" },
                { DatasetMetadata.OperatorUsername, "Operator (Username)" },
                { DatasetMetadata.DsCreatorUsername, string.Empty },
                { DatasetMetadata.Comment, "Comment" },
                { DatasetMetadata.InterestRating, "Interest Rating" },
                { DatasetMetadata.Request, "Request" },
                { DatasetMetadata.WorkPackage, "Work Package" },
                { DatasetMetadata.EusUsageType, "EMSL Usage Type" },
                { DatasetMetadata.EusProposalId, "EMSL Proposal ID" },
                { DatasetMetadata.EusUsers, "EMSL Users List" },
                { DatasetMetadata.CaptureShareName, "Capture Share Name" },
                { DatasetMetadata.CaptureSubdirectory, "Capture Subdirectory" },
                { DatasetMetadata.RunStart, "Run Start" },
                { DatasetMetadata.RunFinish, "Run Finish" }
            };
        }

        /// <summary>
        /// Returns the filename of the trigger file that was loaded
        /// </summary>
        /// <param name="verbose">When false, only return the filename; when true, return the full path to the file</param>
        public override string GetSourceDescription(bool verbose = false)
        {
            return verbose ? TriggerFile.FullName : TriggerFile.Name;
        }


        /// <summary>
        /// Show the dataset creation task ID
        /// </summary>
        public override string ToString()
        {
            return string.Format("XML trigger file {0}", TriggerFile.Name);
        }
    }
}
