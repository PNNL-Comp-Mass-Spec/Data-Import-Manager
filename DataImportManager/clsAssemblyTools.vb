Imports System.IO

Public Class clsAssemblyTools

    Public Shared Sub GetComponentFileVersionInfo()
        Dim ExeFi As New FileInfo(clsGlobal.AppFilePath)
        ' Create a reference to the current directory.
        Dim di As New DirectoryInfo(ExeFi.DirectoryName)

        ' Create an array representing the DLL files in the current directory.
        Dim fiDlls As FileInfo() = di.GetFiles("*.dll")

        ' get file version info for files
        Dim fiTemp As FileInfo
        Dim myFVI As FileVersionInfo
        For Each fiTemp In fiDlls
            myFVI = FileVersionInfo.GetVersionInfo(fiTemp.FullName)
            ''Console.WriteLine(myFVI.ToString)
            DatasetImportManagerBase.clsSummaryFile.Add(myFVI.ToString)
        Next fiTemp

        ' Create an array representing the Exe files in the current directory.
        Dim fiExes As FileInfo() = di.GetFiles("*.exe")

        For Each fiTemp In fiExes
            myFVI = FileVersionInfo.GetVersionInfo(fiTemp.FullName)
            ''Console.WriteLine(myFVI.ToString)
            DatasetImportManagerBase.clsSummaryFile.Add(myFVI.ToString)
        Next fiTemp

    End Sub

End Class
