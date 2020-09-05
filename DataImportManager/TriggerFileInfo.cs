using System;
using System.IO;

namespace DataImportManager
{
    class TriggerFileInfo
    {
        public TriggerFileInfo(FileInfo triggerFile)
        {
            TriggerFile = triggerFile;
            OriginalCaptureSubdirectory = string.Empty;
            FinalCaptureSubdirectory = string.Empty;
            CaptureShareName = string.Empty;
        }

        public FileInfo TriggerFile { get; }

        public string OriginalCaptureSubdirectory { get; set; }

        public string FinalCaptureSubdirectory { get; set; }

        public string CaptureShareName { get; set; }

        public bool NeedsCaptureSubdirectoryReplacement => !OriginalCaptureSubdirectory.Equals(FinalCaptureSubdirectory, StringComparison.OrdinalIgnoreCase);
    }
}
