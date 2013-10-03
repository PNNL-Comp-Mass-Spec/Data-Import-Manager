Imports System.Net.Mail
Imports System.IO
Imports DataImportManager.clsGlobal
Imports System.Collections.Generic
Imports PRISM.Logging
Imports System.Windows.Forms
Imports System.Threading
Imports System.Reflection

Public Class clsMainProcess


#Region "Constants"
	Private Const EMERG_LOG_FILE As String = "DataImportMan_log.txt"
	Private Const MAX_ERROR_COUNT As Integer = 4
#End Region

#Region "Member Variables"
	Private m_MgrSettings As clsMgrSettings
	Private myDataImportTask As clsDataImportTask
	Private m_Logger As ILogger
	Private Shared m_StartupClass As clsMainProcess
	Private m_ConfigChanged As Boolean = False
	Private WithEvents m_FileWatcher As New FileSystemWatcher
	Private m_MgrActive As Boolean = True
	Private m_DebugLevel As Integer = 0
	Private ReadOnly m_XmlFilesToLoad As New List(Of String)
	Public m_db_Err_Msg As String

	Private m_xml_operator_Name As String = String.Empty
	Private m_xml_operator_email As String = String.Empty
	Private m_xml_dataset_path As String = String.Empty
	Private m_xml_instrument_Name As String = String.Empty

	' Keys in this dictionary are instrument names
	' Values are the number of datasets skipped for the given instrument
	Private m_InstrumentsToSkip As Dictionary(Of String, Integer)
	Private myDataXMLValidation As clsXMLTimeValidation
#End Region

	Public Sub New()

	End Sub

	Private Function InitMgr() As Boolean
		Dim LogFile As String
		Dim fiLogFile As FileInfo

		Dim ConnectStr As String
		Dim ModName As String

		'Get the manager settings
		Try
			m_MgrSettings = New clsMgrSettings(EMERG_LOG_FILE)
			If m_MgrSettings.ManagerDeactivated Then
				Return False
			End If
		Catch ex As Exception
			Throw New Exception("clsMainProcess.New(), " & ex.Message)
			'Failures are logged by clsMgrSettings to local emergency log file
		End Try

		Dim FInfo As FileInfo = New FileInfo(GetExePath())
		Try
			'Load initial settings
			m_MgrActive = CBool(m_MgrSettings.GetParam("mgractive"))
			m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

			' create the object that will manage the logging
			LogFile = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("logfilename"))

			' Make sure the folder exists
			Try
				fiLogFile = New FileInfo(LogFile)
				If Not Directory.Exists(fiLogFile.DirectoryName) Then
					Directory.CreateDirectory(fiLogFile.DirectoryName)
				End If
			Catch ex2 As Exception
				Console.WriteLine("Error checking for valid directory for Logfile: " & LogFile)
			End Try

			ConnectStr = m_MgrSettings.GetParam("connectionstring")
			ModName = m_MgrSettings.GetParam("modulename")
			m_Logger = New clsQueLogger(New clsDBLogger(ModName, ConnectStr, LogFile))

			'Write the initial log and status entries
			m_Logger.PostEntry("===== Started Data Import Manager V" & Application.ProductVersion & " =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

		Catch Err As Exception
			Throw New Exception("clsMainProcess.New(), " & Err.Message)
		End Try

		'Setup the logger
		Dim LogFileName As String = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("logfilename"))
		Dim DbLogger As New clsDBLogger
		DbLogger.LogFilePath = LogFileName
		DbLogger.ConnectionString = m_MgrSettings.GetParam("connectionstring")
		DbLogger.ModuleName = m_MgrSettings.GetParam("modulename")
		m_Logger = New clsQueLogger(DbLogger)

		'Set up the FileWatcher to detect setup file changes
		m_FileWatcher = New FileSystemWatcher
		With m_FileWatcher
			.BeginInit()
			.Path = FInfo.DirectoryName
			.IncludeSubdirectories = False
			.Filter = m_MgrSettings.GetParam("configfilename")
			.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
			.EndInit()
			.EnableRaisingEvents = True
		End With

		'Get the debug level
		m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

		m_InstrumentsToSkip = New Dictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

		'Everything worked
		Return True

	End Function

	Shared Sub Main()

		Dim ErrMsg As String

		Try
			AppFilePath = Application.ExecutablePath
			If IsNothing(m_StartupClass) Then
				m_StartupClass = New clsMainProcess
				If Not m_StartupClass.InitMgr() Then Exit Sub
			End If
			m_StartupClass.DoImport()
		Catch Err As Exception
			'Report any exceptions not handled at a lower level to the system application log
			ErrMsg = "Critical exception starting application: " & Err.Message
			PostToEventLog(ErrMsg)
		End Try

	End Sub

	Public Sub DoImport()

		Try

			'Verify an error hasn't left the the system in an odd state
			If DetectStatusFlagFile() Then
				m_Logger.PostEntry("Flag file exists - auto-deleting it", ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
				DeleteStatusFlagFile(m_Logger)
				Exit Sub
			End If

			'Check to see if machine settings have changed
			If m_ConfigChanged Then
				m_ConfigChanged = False
				If Not m_MgrSettings.LoadSettings(True) Then
					If Not String.IsNullOrEmpty(m_MgrSettings.ErrMsg) Then
						'Manager has been deactivated, so report this
						m_Logger.PostEntry(m_MgrSettings.ErrMsg, ILogger.logMsgType.logWarning, True)
					Else
						'Unknown problem reading config file
						m_Logger.PostEntry("Error re-reading config file", ILogger.logMsgType.logError, True)
					End If

					Exit Sub
				End If
				m_FileWatcher.EnableRaisingEvents = True
			End If

			'Check to see if excessive consecutive failures have occurred
			If FailCount > MAX_ERROR_COUNT Then
				'More than MAX_ERROR_COUNT consecutive failures; there must be a generic problem, so exit
				m_Logger.PostEntry("Excessive task failures, disabling manager", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				DisableManagerLocally()
			End If

			'Check to see if the manager is still active
			If Not m_MgrActive Then
				m_Logger.PostEntry("Manager inactive", ILogger.logMsgType.logNormal, True)
				Exit Sub
			End If

			'Check to see if there are any data import files ready
			DoDataImportTask()

		Catch Err As Exception
			m_Logger.PostEntry("Exception in clsMainProcess.DoImport(), " & Err.Message, ILogger.logMsgType.logError, True)
			Exit Sub
		End Try

	End Sub

	Private Sub DoDataImportTask()

		Dim result As ITaskParams.CloseOutType
		Dim blnSuccess As Boolean
		Dim DelBadXmlFilesDays As Integer = CInt(m_MgrSettings.GetParam("deletebadxmlfiles"))
		Dim DelGoodXmlFilesDays As Integer = CInt(m_MgrSettings.GetParam("deletegoodxmlfiles"))
		Dim successFolder As String = m_MgrSettings.GetParam("successfolder")
		Dim failureFolder As String = m_MgrSettings.GetParam("failurefolder")
		Dim mail_msg As String

		Try

			result = ScanXferDirectory()

			If result = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS And m_XmlFilesToLoad.Count > 0 Then

				CreateStatusFlagFile()	  'Set status file for control of future runs


				' create the object that will import the Data record
				'
				myDataImportTask = New clsDataImportTask(m_MgrSettings, m_Logger)
				'
				Application.DoEvents()

				' Add a delay
				Dim importDelay As String = m_MgrSettings.GetParam("importdelay")
				If Environment.MachineName.ToLower().StartsWith("monroe") Then
					importDelay = "1"
				End If
				Thread.Sleep(CInt(importDelay) * 1000)

				' Randomize order of files in m_XmlFilesToLoad
				Dim lstRandomizedFileList As List(Of String)
				lstRandomizedFileList = RandomizeList(m_XmlFilesToLoad)

				For Each XMLFilePath As String In lstRandomizedFileList
					' Validate the xml file

					m_db_Err_Msg = String.Empty

					m_Logger.PostEntry("Starting data import task for dataset: " & XMLFilePath, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

					If ValidateXMLFileMain(XMLFilePath) Then

						If Not File.Exists(XMLFilePath) Then
							m_Logger.PostEntry("XML file no longer exists; cannot import: " & XMLFilePath, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
						Else

							If m_DebugLevel >= 2 Then
								m_Logger.PostEntry("Posting Dataset XML file to database: " & Path.GetFileName(XMLFilePath), ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
							End If

							m_db_Err_Msg = String.Empty
							blnSuccess = myDataImportTask.PostTask(XMLFilePath)

							m_db_Err_Msg = myDataImportTask.DBErrorMessage

							If m_db_Err_Msg.Contains("Timeout expired.") Then
								'post the error and leave the file for another attempt
								m_Logger.PostEntry("Encountered database timeout error for dataset: " & XMLFilePath, ILogger.logMsgType.logError, LOG_DATABASE)
							Else
								If blnSuccess Then
									MoveXmlFile(XMLFilePath, successFolder)
								Else
									Dim moveLocPath = MoveXmlFile(XMLFilePath, failureFolder)
									m_Logger.PostEntry("Error posting xml file to database: " & myDataImportTask.PostTaskErrorMessage & ". View details in log at " & GetLogFileSharePath() & " for: " & moveLocPath, ILogger.logMsgType.logError, LOG_DATABASE)
									mail_msg = "There is a problem with the following XML file: " & moveLocPath & ".  Check the log for details."
									mail_msg &= ControlChars.NewLine & "Operator: " & m_xml_operator_Name

									' Send m_db_Err_Msg to see if there is a suggested solution in table T_DIM_Error_Solution for the error 
									' If a solution is found, then m_db_Err_Msg will get auto-updated with the suggested course of action
									myDataImportTask.GetDbErrorSolution(m_db_Err_Msg)

									' Send an e-mail
									CreateMail(mail_msg, m_xml_operator_email, " - Database error.")
								End If
								m_Logger.PostEntry("Completed Data import task for dataset: " & XMLFilePath, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
							End If
						End If

					End If

				Next
				m_XmlFilesToLoad.Clear()

			Else
				m_Logger.PostEntry("No Data Files to import.", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
				Exit Sub
			End If

			For Each kvItem As KeyValuePair(Of String, Integer) In m_InstrumentsToSkip
				Dim strMessage As String = "Skipped " & kvItem.Value & " dataset"
				If kvItem.Value <> 1 Then strMessage &= "s"
				strMessage &= " for instrument " & kvItem.Key & " due to network errors"
				m_Logger.PostEntry(strMessage, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
			Next

			'Remove successful XML files older than x days
			DeleteXmlFiles(successFolder, DelGoodXmlFilesDays)

			'Remove failed XML files older than x days
			DeleteXmlFiles(failureFolder, DelBadXmlFilesDays)

			' If we got to here, then closeout the task as a success
			'
			DeleteStatusFlagFile(m_Logger)
			FailCount = 0
			m_Logger.PostEntry("Completed task ", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

		Catch Err As Exception
			FailCount += 1
			m_Logger.PostEntry("clsMainProcess.DoDataImportTask(), " & Err.Message, ILogger.logMsgType.logError, True)
			Exit Sub
		End Try

	End Sub

	Public Function ScanXferDirectory() As ITaskParams.CloseOutType

		'Copies the results to the transfer directory
		Dim ServerXferDir As String = m_MgrSettings.GetParam("xferdir")

		'Verify transfer directory exists
		If Not Directory.Exists(ServerXferDir) Then
			'There's a serious problem is the xfer directory can't be found!!!
			m_Logger.PostEntry("Xml transfer folder not found: " & ServerXferDir, ILogger.logMsgType.logError, LOG_DATABASE)
			Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Clear the string dictionary
		m_XmlFilesToLoad.Clear()

		'Load all the Xml File names and dates in the transfer directory into a string dictionary
		Try
			Dim XmlFilesToImport() As String = Directory.GetFiles(ServerXferDir, "*.xml")
			For Each XmlFile As String In XmlFilesToImport
				m_XmlFilesToLoad.Add(XmlFile)
			Next
		Catch err As Exception
			m_Logger.PostError("Error loading Xml Data files from " & ServerXferDir, err, LOG_DATABASE)
			Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Everything must be OK if we got to here
		Return ITaskParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function MoveXmlFile(ByVal xmlFilePath As String, ByVal moveFolder As String) As String

		Dim Fi As FileInfo
		Dim xmlFileNewLoc As String = String.Empty
		Try
			If File.Exists(xmlFilePath) Then
				Fi = New FileInfo(xmlFilePath)
				Dim xmlFileName = Fi.Name
				If Not Directory.Exists(moveFolder) Then
					Directory.CreateDirectory(moveFolder)
				End If

				xmlFileNewLoc = Path.Combine(moveFolder, xmlFileName)
				If File.Exists(xmlFileNewLoc) Then
					File.Delete(xmlFileNewLoc)
				End If
				File.Move(xmlFilePath, xmlFileNewLoc)

			End If
		Catch Err As Exception
			m_Logger.PostEntry("MoveXmlFile, " & Err.Message, ILogger.logMsgType.logError, True)
		End Try
		Return xmlFileNewLoc
	End Function

	Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As FileSystemEventArgs) Handles m_FileWatcher.Changed
		m_ConfigChanged = True
		If m_DebugLevel > 3 Then
			m_Logger.PostEntry("Config file changed", ILogger.logMsgType.logDebug, True)
		End If
		m_FileWatcher.EnableRaisingEvents = False  'Turn off change detection until current change has been acted upon
	End Sub

	Function CreateMail(ByVal mailMsg As String, ByVal addtnlRecipient As String, ByVal subjectAppend As String) As Boolean
		Dim addMsg As String
		Dim enableEmail As Boolean
		Dim mailRecipients As String

		enableEmail = CBool(m_MgrSettings.GetParam("enableemail"))
		If enableEmail Then
			Try
				addMsg = ControlChars.NewLine & ControlChars.NewLine & "(NOTE: This message was sent from an account that is not monitored. If you have any questions, please reply to the list of recipients directly.)"

				' Create the mail message
				Dim mail As New MailMessage()

				' Set the addresses
				mail.From = New MailAddress(m_MgrSettings.GetParam("from"))

				mailRecipients = m_MgrSettings.GetParam("to")
				For Each emailAddress As String In mailRecipients.Split(";"c)
					mail.To.Add(emailAddress)
				Next

				' Possibly update the e-mail address for addtnlRecipient
				If Not String.IsNullOrEmpty(addtnlRecipient) Then
					mail.To.Add(addtnlRecipient)
					mailRecipients &= ";" & addtnlRecipient
				End If

				' Set the Subject and Body
				If String.IsNullOrEmpty(subjectAppend) Then
					mail.Subject = m_MgrSettings.GetParam("subject")
				Else
					mail.Subject = m_MgrSettings.GetParam("subject") + subjectAppend
				End If
				mail.Body = mailMsg & ControlChars.NewLine & ControlChars.NewLine & m_db_Err_Msg & addMsg

				m_Logger.PostEntry("E-mailing " & mailRecipients & " regarding " & m_xml_dataset_path, ILogger.logMsgType.logDebug, True)

				' Send the message
				Dim smtp As New SmtpClient(m_MgrSettings.GetParam("smtpserver"))
				smtp.Send(mail)

			Catch Ex As Exception
				m_Logger.PostEntry("Error sending email message: " & Ex.Message, ILogger.logMsgType.logError, True)
			End Try
		End If

	End Function

	Private Function DeleteXmlFiles(ByVal FileDirectory As String, ByVal NoDays As Integer) As Boolean

		Dim filedate As DateTime
		Dim daysDiff As Integer
		Dim workDirectory As String

		workDirectory = FileDirectory
		'Verify directory exists
		If Not Directory.Exists(workDirectory) Then
			'There's a serious problem if the success/failure directory can't be found!!!
			m_Logger.PostEntry("Xml success/failure folder not found.", ILogger.logMsgType.logError, LOG_DATABASE)
			Return False
		End If

		'Load all the Xml File names and dates in the transfer directory into a string dictionary
		Try
			Dim XmlFilesToDelete() As String = Directory.GetFiles(Path.Combine(workDirectory, workDirectory))
			For Each XmlFile As String In XmlFilesToDelete
				filedate = File.GetLastWriteTimeUtc(XmlFile)
				daysDiff = DateTime.UtcNow.Subtract(filedate).Days
				If daysDiff > NoDays Then
					File.Delete(XmlFile)
				End If
			Next
		Catch err As Exception
			m_Logger.PostError("Error deleting old Xml Data files at " & FileDirectory, err, LOG_DATABASE)
			Return False
		End Try

		'Everything must be OK if we got to here
		Return True

	End Function

	Private Sub DisableManagerLocally()

		If Not m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False") Then
			m_Logger.PostEntry("Error while disabling manager: " & m_MgrSettings.ErrMsg, ILogger.logMsgType.logError, True)
		End If

	End Sub

	Private Function GetExePath() As String
		' Could use Application.ExecutablePath
		' Instead, use reflection
		Return Assembly.GetExecutingAssembly().Location
	End Function

	''' <summary>
	''' Returns a string with the path to the log file, assuming the file can be access with \\ComputerName\DMS_Programs\ProgramFolder\Logs\LogFileName.txt
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function GetLogFileSharePath() As String

		Dim strLogFilePath As String

		Dim FInfo As FileInfo = New FileInfo(GetExePath())

		strLogFilePath = Path.Combine(FInfo.Directory.Name, m_MgrSettings.GetParam("logfilename"))

		' strLogFilePath should look like this:
		'	DataImportManager\Logs\DataImportManager

		' Prepend the computer name and share name, giving a string similar to:
		' \\proto-3\DMS_Programs\DataImportManager\Logs\DataImportManager

		strLogFilePath = "\\" & Environment.MachineName & "\DMS_Programs\" & strLogFilePath

		' Append the date stamp to the log
		strLogFilePath &= "_" & DateTime.Now.ToString("MM-dd-yyyy") & ".txt"

		Return strLogFilePath

	End Function

	Private Function RandomizeList(ByVal lstItemsToRandomize As IEnumerable(Of String)) As List(Of String)

		Dim lstRandomized As New List(Of String)
		Dim intIndex As Integer
		Dim objRand As New Random()

		Dim lstCandidates As List(Of String) = lstItemsToRandomize.ToList()

		Do While lstCandidates.Count > 0
			intIndex = objRand.Next(0, lstCandidates.Count - 1)

			lstRandomized.Add(lstCandidates.Item(intIndex))
			lstCandidates.RemoveAt(intIndex)
		Loop

		Return lstRandomized
	End Function

	Private Shared Sub PostToEventLog(ByVal ErrMsg As String)
		Const EVENT_LOG_NAME As String = "DMSDataImportManager"

		Try
			Console.WriteLine()
			Console.WriteLine("===============================================================")
			Console.WriteLine(ErrMsg)
			Console.WriteLine("===============================================================")
			Console.WriteLine()
			Console.WriteLine("You may need to run this application once from an elevated (administrative level) command prompt so that it can create the " & EVENT_LOG_NAME & " application log")
			Console.WriteLine()

			Dim Ev As New EventLog("Application", ".", EVENT_LOG_NAME)
			Trace.Listeners.Add(New EventLogTraceListener(EVENT_LOG_NAME))
			Trace.WriteLine(ErrMsg)
			Ev.Close()

		Catch ex As Exception
			Console.WriteLine()
			Console.WriteLine("Exception logging to the event log: " & ex.Message)
		End Try

		Thread.Sleep(500)

	End Sub

	''' <summary>
	''' Adds or updates strInstrumentName in m_InstrumentsToSkip
	''' </summary>
	''' <param name="strInstrumentName"></param>
	''' <remarks></remarks>
	Private Sub UpdateInstrumentsToSkip(ByVal strInstrumentName As String)

		Dim intDatasetsSkipped As Integer = 0
		If m_InstrumentsToSkip.TryGetValue(strInstrumentName, intDatasetsSkipped) Then
			m_InstrumentsToSkip(strInstrumentName) = intDatasetsSkipped + 1
		Else
			m_InstrumentsToSkip.Add(strInstrumentName, 1)
		End If

	End Sub
	''' <summary>
	''' Process the specified XML file
	''' </summary>
	''' <param name="xmlFilePath">XML file to process</param>
	''' <returns>True if XML file is valid and dataset is ready for import; otherwise false</returns>
	''' <remarks></remarks>
	Private Function ValidateXMLFileMain(ByVal xmlFilePath As String) As Boolean

		Try
			Dim xmlRslt As IXMLValidateStatus.XmlValidateStatus
			Dim timeValFolder As String = m_MgrSettings.GetParam("timevalidationfolder")
			Dim moveLocPath As String
			Dim mail_msg As String
			Dim failureFolder As String = m_MgrSettings.GetParam("failurefolder")
			Dim rslt As Boolean
			myDataImportTask = New clsDataImportTask(m_MgrSettings, m_Logger)

			myDataXMLValidation = New clsXMLTimeValidation(m_MgrSettings, m_Logger, m_InstrumentsToSkip)

			xmlRslt = myDataXMLValidation.ValidateXMLFile(xmlFilePath)

			m_xml_operator_Name = myDataXMLValidation.OperatorName()
			m_xml_operator_email = myDataXMLValidation.OperatorEMail()
			m_xml_dataset_path = myDataXMLValidation.DatasetPath()
			m_xml_instrument_Name = myDataXMLValidation.InstrumentName()

			If xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED Then
				moveLocPath = MoveXmlFile(xmlFilePath, timeValFolder)
				m_Logger.PostEntry("XML Time validation error, file " & moveLocPath, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
				m_Logger.PostEntry("Time validation error. View details in log at " & GetLogFileSharePath() & " for: " & moveLocPath, ILogger.logMsgType.logError, LOG_DATABASE)
				mail_msg = "Operator: " & m_xml_operator_Name & ControlChars.NewLine
				mail_msg &= "There was a time validation error with the following XML file: " & ControlChars.NewLine & moveLocPath & ControlChars.NewLine
				mail_msg &= "Check the log for details.  " & ControlChars.NewLine
				mail_msg &= "Dataset filename and location: " + m_xml_dataset_path
				CreateMail(mail_msg, m_xml_operator_email, " - Time validation error.")
				Return False

			ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR Then
				moveLocPath = MoveXmlFile(xmlFilePath, failureFolder)
				m_Logger.PostEntry("An error was encountered during the validation process, file " & moveLocPath, ILogger.logMsgType.logWarning, LOG_DATABASE)
				mail_msg = "XML error encountered during validation process for the following XML file: " & ControlChars.NewLine & moveLocPath & ControlChars.NewLine
				mail_msg &= "Check the log for details.  " & ControlChars.NewLine
				mail_msg &= "Dataset filename and location: " + m_xml_dataset_path
				CreateMail(mail_msg, m_xml_operator_email, " - XML validation error.")
				Return False

			ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE Then
				' Logon failure; Do not move the XML file
				Return False
			ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR Then
				' Network error; Do not move the XML file
				' Furthermore, do not process any more .XML files for this instrument
				UpdateInstrumentsToSkip(m_xml_instrument_Name)
				Return False

			ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SKIP_INSTRUMENT Then
				UpdateInstrumentsToSkip(m_xml_instrument_Name)
				Return False

			ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_WAIT_FOR_FILES Then
				Return False

			ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED Then
				' Size changed; Do not move the XML file
				Return False

			ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA Then
				moveLocPath = MoveXmlFile(xmlFilePath, failureFolder)
				m_Logger.PostEntry("Dataset " & myDataXMLValidation.DatasetName & " not found at " & myDataXMLValidation.SourcePath, ILogger.logMsgType.logWarning, LOG_DATABASE)
				mail_msg = "Operator: " & m_xml_operator_Name & ControlChars.NewLine
				mail_msg &= "The dataset data is not available for capture and was not added to DMS for dataset: " & ControlChars.NewLine & moveLocPath & ControlChars.NewLine
				mail_msg &= "Check the log for details.  " & ControlChars.NewLine
				mail_msg &= "Dataset not found in following location: " + m_xml_dataset_path
				m_db_Err_Msg = "The dataset data is not available for capture"
				rslt = myDataImportTask.GetDbErrorSolution(m_db_Err_Msg)
				If Not rslt Then
					m_db_Err_Msg = String.Empty
				End If
				CreateMail(mail_msg, m_xml_operator_email, " - Dataset not found.")
				Return False
			Else
				' xmlRslt is one of the following:
				' We'll return "True" below

				' XML_VALIDATE_SUCCESS
				' XML_VALIDATE_NO_CHECK
				' XML_VALIDATE_CONTINUE
				' XML_VALIDATE_SKIP_INSTRUMENT

			End If
		Catch ex As Exception
			m_Logger.PostError("Error validating Xml Data file, file " & xmlFilePath, ex, LOG_DATABASE)
			Return False
		End Try

		Return True
	End Function

End Class
