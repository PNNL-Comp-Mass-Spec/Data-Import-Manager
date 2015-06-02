'Contains functions/variables common to all parts of the Analysis Manager
Imports System.IO
Imports PRISM.Logging
Imports System.Text
Imports System.Reflection

Public Class clsGlobal

	'Constants
	Public Const LOG_LOCAL_ONLY As Boolean = True
	Public Const LOG_DATABASE As Boolean = False
	Public Shared FailCount As Integer = 0

	Public Shared Sub CreateStatusFlagFile()

		'Creates a dummy file in the application directory to be used for controlling task request
		'	bypass

        Dim ExeFi As New FileInfo(GetExePath())
		Dim TestFileFi As New FileInfo(Path.Combine(ExeFi.DirectoryName, "FlagFile.txt"))
		Dim Sw As StreamWriter = TestFileFi.AppendText()

		Sw.WriteLine(DateTime.Now().ToString)
		Sw.Flush()
		Sw.Close()

    End Sub
    
	Public Shared Sub DeleteStatusFlagFile(ByVal MyLogger As ILogger)

		'Deletes the task request control flag file
        Dim ExeFi As New FileInfo(GetExePath())
		Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, "FlagFile.txt")

		Try
			If File.Exists(TestFile) Then
				File.Delete(TestFile)
			End If
        Catch ex As Exception
            MyLogger.PostEntry("DeleteStatusFlagFile, " & ex.Message, ILogger.logMsgType.logError, True)
        End Try

    End Sub

    Public Shared Function DetectStatusFlagFile() As Boolean

        ' Returns True if task request control flag file exists
        Dim ExeFi As New FileInfo(GetExePath())
        Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, "FlagFile.txt")

        Return File.Exists(TestFile)

    End Function

    Public Shared Function GetExePath() As String
        ' Could use Application.ExecutablePath
        ' Instead, use reflection
        Return Assembly.GetExecutingAssembly().Location
    End Function


    Public Shared Function LoadXmlFileContentsIntoString(ByVal triggerFile As FileInfo, ByVal MyLogger As ILogger) As String
        Try
            ' Read the contents of the xml file into a string which will be passed into a stored procedure.
            If Not triggerFile.Exists Then
                MyLogger.PostEntry("clsGlobal.LoadXmlFileContentsIntoString(), File: " & triggerFile.FullName & " does not exist.", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                Return String.Empty
            End If
            Dim xmlFileContents = New StringBuilder
            Using sr = New StreamReader(New FileStream(triggerFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                Do While Not sr.EndOfStream
                    Dim input = sr.ReadLine()
                    If xmlFileContents.Length > 0 Then xmlFileContents.Append(Environment.NewLine)
                    xmlFileContents.Append(input.Replace("&", "&#38;"))
                Loop
            End Using
            Return xmlFileContents.ToString()
        Catch ex As Exception
            MyLogger.PostEntry("clsGlobal.LoadXmlFileContentsIntoString(), Error reading xml file, " & ex.Message, ILogger.logMsgType.logError, True)
            Return String.Empty
        End Try

    End Function

End Class
