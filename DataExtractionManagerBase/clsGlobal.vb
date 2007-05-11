'Contains functions/variables common to all parts of the Analysis Manager
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports PRISM.Logging

Public Class clsGlobal

	'Constants
    Public Const LOG_LOCAL_ONLY As Boolean = True
	Public Const LOG_DATABASE As Boolean = False
	Public Shared FailCount As Integer = 0
	Public Shared AppFilePath As String = ""

    Public Shared Sub CreateStatusFlagFile()

        'Creates a dummy file in the application directory to be used for controlling task request
        '	bypass

        Dim ExeFi As New FileInfo(AppFilePath)
        Dim TestFileFi As New FileInfo(Path.Combine(ExeFi.DirectoryName, "FlagFile.txt"))
        Dim Sw As StreamWriter = TestFileFi.AppendText()

        Sw.WriteLine(Now().ToString)
        Sw.Flush()
        Sw.Close()

        Sw = Nothing
        TestFileFi = Nothing
        ExeFi = Nothing

    End Sub

    Public Shared Sub DeleteStatusFlagFile(ByVal MyLogger As ILogger)

        'Deletes the task request control flag file
        Dim ExeFi As New FileInfo(AppFilePath)
        Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, "FlagFile.txt")

        Try
            If File.Exists(TestFile) Then
                File.Delete(TestFile)
            End If
        Catch Err As System.Exception
            MyLogger.PostEntry("DeleteStatusFlagFile, " & Err.Message, ILogger.logMsgType.logError, True)
        End Try

    End Sub

    Public Shared Function DetectStatusFlagFile() As Boolean

        'Returns True if task request control flag file exists
        Dim ExeFi As New FileInfo(AppFilePath)
        Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, "FlagFile.txt")

        Return File.Exists(TestFile)

    End Function

End Class
