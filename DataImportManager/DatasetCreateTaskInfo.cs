using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace DataImportManager
{
    internal class DatasetCreateTaskInfo : DatasetCaptureInfo
    {
        /// <summary>
        /// Dictionary mapping dataset metadata enum values to the XML element names in the XML obtained from procedure request_dataset_create_task
        /// </summary>
        public Dictionary<DatasetMetadata, string> CreateTaskXmlNames { get; }

        /// <summary>
        /// Dataset creation task queue ID
        /// </summary>
        public int TaskID { get; }

        /// <summary>
        /// XML parameters obtained from procedure request_dataset_create_task
        /// </summary>
        public string XmlParameters { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="taskID">Dataset creation task ID (column entry_id in t_dataset_create_queue)</param>
        /// <param name="xmlParameters"></param>
        public DatasetCreateTaskInfo(int taskID, string xmlParameters)
        {
            CreateTaskXmlNames = new Dictionary<DatasetMetadata, string>();

            TaskID = taskID;
            XmlParameters = xmlParameters;

            DefineCreateTaskXmlNameMap();
        }

        private void AddTriggerFileParameter(
            XDocument doc,
            IReadOnlyDictionary<DatasetMetadata, string> datasetInfo,
            IReadOnlyDictionary<DatasetMetadata, string> triggerFileParamNames,
            DatasetMetadata metadataItem)
        {
            if (doc.Root == null)
            {
                throw new Exception("Root element for the XML is null");
            }

            if (!triggerFileParamNames.TryGetValue(metadataItem, out var paramName))
            {
                LogWarning(string.Format("Dictionary triggerFileParamNames does not have key {0}", metadataItem.ToString()));
                return;
            }

            var paramValue = datasetInfo.TryGetValue(metadataItem, out var value) ? value : string.Empty;

            // Add a new node, e.g.
            // <Parameter Name="Experiment Name" Value="QC_Mam_23_01" />

            doc.Root.Add(new XElement("Parameter",
                new XAttribute("Name", paramName),
                new XAttribute("Value", paramValue)));
        }

        /// <summary>
        /// Converts dataset create queue XML into a dictionary
        /// </summary>
        /// <param name="datasetInfo">Dictionary of dataset metadata</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ConvertCreateQueueXMLToDictionary(out Dictionary<DatasetMetadata, string> datasetInfo)
        {
            // ReSharper disable CommentTypo

            // Example contents of XmlParameters

            // <root>
            //   <dataset>SW_Test_Dataset_2023-10-24d</dataset>
            //   <experiment>QC_Mam_23_01</experiment>
            //   <instrument>Exploris03</instrument>
            //   <separation_type>LC-Dionex-Formic_100min</separation_type>
            //   <lc_cart>Birch</lc_cart>
            //   <lc_cart_config>Birch_BEH-1pt7</lc_cart_config>
            //   <lc_column>WBEH-CoAnn-23-09-02</lc_column>
            //   <wellplate></wellplate>
            //   <well>B5</well>
            //   <dataset_type>HMS-HCD-HMSn</dataset_type>
            //   <operator_username>D3L243</operator_username>
            //   <ds_creator_username>D3L243</ds_creator_username>
            //   <comment>Test comment</comment>
            //   <interest_rating>Released</interest_rating>
            //   <request>0</request>
            //   <work_package>none</work_package>
            //   <eus_usage_type>USER_ONSITE</eus_usage_type>
            //   <eus_proposal_id>60328</eus_proposal_id>
            //   <eus_users>35357</eus_users>
            //   <capture_share_name>ProteomicsData2</capture_share_name>
            //   <capture_subdirectory>Soil</capture_subdirectory>
            //   <command>add</command>
            // </root>

            // ReSharper restore CommentTypo

            datasetInfo = new Dictionary<DatasetMetadata, string>();

            var doc = XDocument.Parse(XmlParameters);
            var elements = doc.Elements("root").ToList();

            try
            {
                foreach (var item in CreateTaskXmlNames)
                {
                    var elementName = item.Value;

                    var value = GetXmlValue(elements, elementName);

                    datasetInfo.Add(item.Key, value);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error parsing dataset creation task XML to generate the equivalent trigger file XML", ex);
                return false;
            }
        }

        private void DefineCreateTaskXmlNameMap()
        {
            CreateTaskXmlNames.Clear();

            CreateTaskXmlNames.Add(DatasetMetadata.Dataset, "dataset");
            CreateTaskXmlNames.Add(DatasetMetadata.Experiment, "experiment");
            CreateTaskXmlNames.Add(DatasetMetadata.Instrument, "instrument");
            CreateTaskXmlNames.Add(DatasetMetadata.SeparationType, "separation_type");
            CreateTaskXmlNames.Add(DatasetMetadata.LcCart, "lc_cart");
            CreateTaskXmlNames.Add(DatasetMetadata.LcCartConfig, "lc_cart_config");
            CreateTaskXmlNames.Add(DatasetMetadata.LcColumn, "lc_column");
            CreateTaskXmlNames.Add(DatasetMetadata.Wellplate, "wellplate");
            CreateTaskXmlNames.Add(DatasetMetadata.Well, "well");
            CreateTaskXmlNames.Add(DatasetMetadata.DatasetType, "dataset_type");
            CreateTaskXmlNames.Add(DatasetMetadata.OperatorUsername, "operator_username");
            CreateTaskXmlNames.Add(DatasetMetadata.DsCreatorUsername, "ds_creator_username");
            CreateTaskXmlNames.Add(DatasetMetadata.Comment, "comment");
            CreateTaskXmlNames.Add(DatasetMetadata.InterestRating, "interest_rating");
            CreateTaskXmlNames.Add(DatasetMetadata.Request, "request");
            CreateTaskXmlNames.Add(DatasetMetadata.WorkPackage, "work_package");
            CreateTaskXmlNames.Add(DatasetMetadata.EusUsageType, "eus_usage_type");
            CreateTaskXmlNames.Add(DatasetMetadata.EusProposalId, "eus_proposal_id");
            CreateTaskXmlNames.Add(DatasetMetadata.EusUsers, "eus_users");
            CreateTaskXmlNames.Add(DatasetMetadata.CaptureShareName, "capture_share_name");
            CreateTaskXmlNames.Add(DatasetMetadata.CaptureSubdirectory, "capture_subdirectory");
        }

        public bool GetXmlTriggerFileParameters(out string triggerFileXML)
        {
            var success = ConvertCreateQueueXMLToDictionary(out var datasetInfo);

            if (!success)
            {
                LogError("ConvertCreateQueueXMLToDictionary returned false in call to GetXmlTriggerFileParameters for " + GetSourceDescription());
                triggerFileXML = string.Empty;
                return false;
            }

            var doc = new XDocument(
                new XDeclaration("1.0", "utf-8", "yes"),
                new XElement("Dataset"));

            var triggerFileParamNames = TriggerFileInfo.GetTriggerFileParamNameMap();

            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.Dataset);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.Experiment);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.Instrument);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.CaptureShareName);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.CaptureSubdirectory);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.SeparationType);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.LcCart);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.LcCartConfig);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.LcColumn);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.DatasetType);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.OperatorUsername);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.WorkPackage);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.Comment);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.InterestRating);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.Request);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.EusProposalId);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.EusUsageType);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.EusUsers);
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.RunStart);  // Note that the parameter value will be an empty string
            AddTriggerFileParameter(doc, datasetInfo, triggerFileParamNames, DatasetMetadata.RunFinish); // Note that the parameter value will be an empty string

            var settings = new XmlWriterSettings
            {
                Indent = true,
                IndentChars = "  ",
                OmitXmlDeclaration = true
            };

            using var sw = new StringWriter();
            using (var xmlWriter = XmlWriter.Create(sw, settings))
            {
                doc.Save(xmlWriter);
            }

            triggerFileXML = sw.ToString();

            return true;
        }

        /// <summary>
        /// Extract the string value inside an XML element
        /// </summary>
        /// <param name="elementList"></param>
        /// <param name="elementName"></param>
        /// <param name="valueIfMissing"></param>
        /// <returns>String value, or valueIfMissing if a parse error</returns>
        public static string GetXmlValue(IEnumerable<XElement> elementList, string elementName, string valueIfMissing = "")
        {
            var elements = elementList.Elements(elementName).ToList();

            if (elements.Count == 0)
                return valueIfMissing;

            var firstElement = elements[0];

            return string.IsNullOrEmpty(firstElement?.Value) ? valueIfMissing : firstElement.Value;
        }

        /// <summary>
        /// Returns the filename of the trigger file that was loaded
        /// </summary>
        /// <param name="verbose">Not used by this method</param>
        public override string GetSourceDescription(bool verbose = false)
        {
            return string.Format("Dataset creation task queue ID {0}", TaskID);
        }

        /// <summary>
        /// Show the dataset creation task ID
        /// </summary>
        public override string ToString()
        {
            return string.Format("Dataset create task {0}", TaskID);
        }
    }
}
