using PRISM;

namespace DataImportManager
{
    internal class CommandLineOptions
    {
        // Ignore Spelling: bionet

        [Option("NoMail", HelpShowsDefault = false, HelpText = "Disable sending e-mail when errors are encountered")]
        public bool NoMailMode { get; set; }

        [Option("Preview", HelpShowsDefault = false, HelpText = "Enable preview mode, where we report any trigger files found, " +
                                                                 "but do not post them to DMS and do not move them to the failure directory if there is an error. " +
                                                                 "Using /Preview forces /NoMail and /Trace to both be enabled")]
        public bool PreviewMode { get; set; }

        [Option("Trace", HelpShowsDefault = false, HelpText = "Enable trace mode, where debug messages are written to the command prompt")]
        public bool TraceMode { get; set; }

        [Option("ISE", HelpShowsDefault = false, HelpText = "Ignore instrument source check errors (e.g. cannot access bionet)")]
        public bool IgnoreInstrumentSourceErrors { get; set; }

        public bool Validate()
        {
            if (PreviewMode)
            {
                NoMailMode = true;
                TraceMode = true;
            }

            return true;
        }
    }
}
