# Data Import Manager

The Data Import Manager is a part of PRISM, the Proteomics Research Information and Management System.
The Data Import Manager monitors a server share for new dataset trigger files, which are
XML files that specify metadata for new datasets to import into the Data Management System (DMS).

The Data Import Manager validates the metadata in the XML trigger file, notifying the instrument
operator by e-mail if invalid metadata is encountered.  After processing each XML trigger file, 
the file is moved into either a success or failure directory.

## Example XML Trigger File

```xml
<?xml version="1.0"?>
<Dataset>
  <Parameter Name="Dataset Name" Value="QC_Shew_17_01-run03-HCD-Top-6_14Nov17_Merry_17-10-07" />
  <Parameter Name="Experiment Name" Value="QC_Shew_17_01" />
  <Parameter Name="Instrument Name" Value="VOrbiETD02" />
  <Parameter Name="Capture Subfolder" Value="" />
  <Parameter Name="Separation Type" Value="LC-Waters-Formic_100min" />
  <Parameter Name="LC Cart Name" Value="Merry" />
  <Parameter Name="LC Cart Config" Value="Merry_Jup_Peptides_5uL" />
  <Parameter Name="LC Column" Value="17-10-07" />
  <Parameter Name="Wellplate Number" Value="" />
  <Parameter Name="Well Number" Value="0" />
  <Parameter Name="Dataset Type" Value="HMS-HCD-HMSn" />
  <Parameter Name="Operator (PRN)" Value="Moore, Ronald J" />
  <Parameter Name="Comment" Value="" />
  <Parameter Name="Interest Rating" Value="Unreviewed" />
  <Parameter Name="Request" Value="0" />
  <Parameter Name="EMSL Proposal ID" Value="" />
  <Parameter Name="EMSL Usage Type" Value="MAINTENANCE" />
  <Parameter Name="EMSL Users List" Value="" />
  <Parameter Name="Run Start" Value="11/14/2017 13:24:01" />
  <Parameter Name="Run Finish" Value="11/14/2017 15:52:03" />
</Dataset>
```

## Syntax 

```
DataImportManager.exe [/NoMail] [/Preview] [/Trace] [/ISE]
```

## Command Line Arguments

Use `/NoMail` to disable sending e-mail when errors are encountered

Use `/Preview` to enable preview mode, where the program reports any trigger files found, but does not post them to DMS and does not move them to the failure directory if there is an error
* Using `/Preview` forces `/NoMail` and `/Trace` to both be enabled

Use `/Trace` to display additional debug messages

Use `/ISE` to ignore instrument source check errors (e.g., cannot access bionet)

## Contacts

Written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: proteomics@pnnl.gov \
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics

## License

The Data Import Manager is licensed under the Apache License, Version 2.0; 
you may not use this program except in compliance with the License.  You may obtain 
a copy of the License at https://opensource.org/licenses/Apache-2.0
