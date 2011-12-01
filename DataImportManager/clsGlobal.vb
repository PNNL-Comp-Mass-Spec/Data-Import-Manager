'Contains functions/variables common to all parts of the Analysis Manager
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports PRISM.Logging

Public Class clsGlobal

	'Constants
	Public Const LOG_LOCAL_ONLY As Boolean = True
	Public Const LOG_DATABASE As Boolean = False
	Public Shared FailCount As Integer = 0
	Public Shared AppFilePath As String = String.Empty

	Public Shared Sub CreateStatusFlagFile()

		'Creates a dummy file in the application directory to be used for controlling task request
		'	bypass

		Dim ExeFi As New FileInfo(AppFilePath)
		Dim TestFileFi As New FileInfo(Path.Combine(ExeFi.DirectoryName, "FlagFile.txt"))
		Dim Sw As StreamWriter = TestFileFi.AppendText()

		Sw.WriteLine(System.DateTime.Now().ToString)
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

	Public Shared Function LoadXmlFileContentsIntoString(ByVal xmlFilePath As String, ByVal MyLogger As ILogger) As String
		Dim xmlFileContents As String
		Try
			xmlFileContents = String.Empty
			'Read the contents of the xml file into a string which will be passed into a stored procedure.
			If Not File.Exists(xmlFilePath) Then
				MyLogger.PostEntry("clsGlobal.LoadXmlFileContentsIntoString(), File: " & xmlFilePath & " does not exist.", ILogger.logMsgType.logError, True)
				Return String.Empty
			End If
			Dim sr As StreamReader = File.OpenText(xmlFilePath)
			Dim input As String
			input = sr.ReadLine()
			xmlFileContents = input
			While Not input Is Nothing
				input = sr.ReadLine()
				If Not input Is Nothing Then
					input = input.Replace("&", "&#38;")
				End If
				xmlFileContents = xmlFileContents + Chr(13) + Chr(10) + input
			End While
			sr.Close()
			Return xmlFileContents
		Catch Err As System.Exception
			MyLogger.PostEntry("clsGlobal.LoadXmlFileContentsIntoString(), Error reading xml file, " & Err.Message, ILogger.logMsgType.logError, True)
			Return String.Empty
		End Try

	End Function

End Class
