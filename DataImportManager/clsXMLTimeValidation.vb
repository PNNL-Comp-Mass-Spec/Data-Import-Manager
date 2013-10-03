Imports System.Collections.Generic
Imports PRISM.Logging
Imports PRISM.Files
Imports System.Data.SqlClient
Imports System.IO
Imports DataImportManager.clsGlobal
Imports System.Windows.Forms
Imports System.Threading


Public Class clsXMLTimeValidation
	Inherits clsDBTask

	Private m_ins_Name As String = String.Empty
	Private m_dataset_Name As String = String.Empty
	Private m_run_Finish_Utc As DateTime = New DateTime(1960, 1, 1)
	Private m_capture_Type As String = String.Empty
	Private m_source_path As String = String.Empty
	Private m_operator_PRN As String = String.Empty
	Private m_operator_Email As String = String.Empty
	Private m_operator_Name As String = String.Empty
	Private m_dataset_Path As String = String.Empty
	Protected m_ShareConnector As ShareConnector
	Protected m_SleepInterval As Integer = 30

	Protected m_InstrumentsToSkip As Dictionary(Of String, Integer)
	Protected WithEvents m_FileTools As clsFileTools

	Public ReadOnly Property DatasetName() As String
		Get
			Return FixNull(m_dataset_Name)
		End Get
	End Property

	Public ReadOnly Property DatasetPath() As String
		Get
			Return FixNull(m_dataset_Path)
		End Get
	End Property

	Public ReadOnly Property InstrumentName() As String
		Get
			Return m_ins_Name
		End Get
	End Property

	Public ReadOnly Property OperatorEMail() As String
		Get
			Return FixNull(m_operator_Email)
		End Get
	End Property

	Public ReadOnly Property OperatorName() As String
		Get
			Return FixNull(m_operator_Name)
		End Get
	End Property

	Public ReadOnly Property OperatorPRN() As String
		Get
			Return FixNull(m_operator_PRN)
		End Get
	End Property


	Public ReadOnly Property SourcePath() As String
		Get
			Return FixNull(m_source_path)
		End Get
	End Property

#Region "Enums"
	Protected Enum RawDSTypes
		None
		File
		FolderNoExt
		FolderExt
	End Enum
#End Region

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="mgrParams"></param>
	''' <param name="logger"></param>
	''' <remarks></remarks>
	Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger, dctInstrumentsToSkip As Dictionary(Of String, Integer))
		MyBase.New(mgrParams, logger)
		m_FileTools = New clsFileTools
		m_InstrumentsToSkip = dctInstrumentsToSkip
	End Sub

	Private Function FixNull(ByVal strText As String) As String
		If String.IsNullOrEmpty(strText) Then
			Return String.Empty
		Else
			Return strText
		End If
	End Function

	Public Function ValidateXMLFile(ByVal xmlFilePath As String) As IXMLValidateStatus.XmlValidateStatus
		Dim rslt As IXMLValidateStatus.XmlValidateStatus

		m_connection_str = m_mgrParams.GetParam("ConnectionString")

		Try
			rslt = GetXMLParameters(xmlFilePath)
		Catch ex As Exception
			m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), Error reading the XML file " & Path.GetFileName(xmlFilePath) & ": " & ex.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
		End Try

		If rslt <> IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
			Return rslt
		ElseIf m_InstrumentsToSkip.ContainsKey(m_ins_Name) Then
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SKIP_INSTRUMENT
		End If

		Try
			OpenConnection()
		Catch Err As Exception
			m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), error opening connection, " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED
		End Try

		Try

			rslt = SetDbInstrumentParameters(m_ins_Name)
			If rslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
				rslt = PerformValidation()
			Else
				Return rslt
			End If

		Catch Err As Exception
			m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), Error calling PerformValidation, " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
		End Try

		Try
			CLoseConnection()
		Catch Err As Exception
			m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), Error closing connection, " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
		End Try

		Return rslt

	End Function

	'Take the xml file and load into a dataset
	'iterate through the dataset to retrieve the instrument name
	Private Function GetXMLParameters(ByVal xmlFilename As String) As IXMLValidateStatus.XmlValidateStatus
		Dim parameterName As String
		Dim rslt As IXMLValidateStatus.XmlValidateStatus
		Dim xmlFileContents As String
		'initialize return value
		rslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE
		xmlFileContents = LoadXmlFileContentsIntoString(xmlFilename, m_logger)
		'Load into a string reader after '&' was fixed
		Dim xmlStringReader As TextReader
		xmlStringReader = New StringReader(xmlFileContents)
		Try
			Dim xmlDataSet As New DataSet()
			xmlDataSet.ReadXml(xmlStringReader)	 'Everything must be OK if we got to here
			Dim table As DataTable
			For Each table In xmlDataSet.Tables
				Dim row As DataRow
				For Each row In table.Rows
					parameterName = row("Name").ToString()
					Select Case parameterName
						Case "Instrument Name"
							m_ins_Name = row("Value").ToString()
						Case "Dataset Name"
							m_dataset_Name = row("Value").ToString()
						Case "Run Finish UTC"
							m_run_Finish_Utc = CDate(row("Value").ToString())
						Case "Operator (PRN)"
							m_operator_PRN = row("Value").ToString()
					End Select
				Next row
			Next table

			If String.IsNullOrEmpty(m_ins_Name) Then
				m_logger.PostEntry("clsXMLTimeValidation.GetXMLParameters(), The instrument name was blank.", ILogger.logMsgType.logError, True)
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_BAD_XML
			End If

			If m_ins_Name.StartsWith("9T") Or m_ins_Name.StartsWith("11T") Or m_ins_Name.StartsWith("12T") Then
				rslt = InstrumentWaitDelay(xmlFilename)
			End If

			If rslt <> IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
				Return rslt
			Else
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE
			End If

		Catch Err As Exception
			m_logger.PostEntry("clsXMLTimeValidation.GetXMLParameters(), Error reading XML File, " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
		End Try

	End Function

	Private Function InstrumentWaitDelay(ByVal xmlFilename As String) As IXMLValidateStatus.XmlValidateStatus

		Try
			Dim fileModDate As DateTime
			Dim fileModDateDelay As DateTime
			Dim dateNow As DateTime
			Dim delayValue As Integer
			delayValue = CInt(m_mgrParams.GetParam("xmlfiledelay"))
			fileModDate = File.GetLastWriteTimeUtc(xmlFilename)
			fileModDateDelay = fileModDate.AddMinutes(delayValue)
			dateNow = DateTime.UtcNow
			If dateNow < fileModDateDelay Then
				m_logger.PostEntry("clsXMLTimeValidation.InstrumentWaitDelay(), The dataset import is being delayed for XML File: " + xmlFilename, ILogger.logMsgType.logError, True)
				Return IXMLValidateStatus.XmlValidateStatus.XML_WAIT_FOR_FILES
			End If

			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE

		Catch Err As Exception
			m_logger.PostEntry("clsXMLTimeValidation.InstrumentWaitDelay(), Error determining wait delay, " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
		End Try

	End Function

	'Query to get the instrument data from the database and then 
	'iterate through the dataset to retrieve the capture type and source path
	Private Function SetDbInstrumentParameters(ByVal insName As String) As IXMLValidateStatus.XmlValidateStatus

		Try
			'Requests additional task parameters from database and adds them to the m_taskParams string dictionary
			Dim SQL As String
			SQL = "SELECT Name, Class, RawDataType, Capture, SourcePath " & _
			   " FROM dbo.V_Instrument_List_Export " & _
			   " WHERE Name = '" + insName + "'" & _
			   " ORDER BY Name "

			'Get a list of all records in database (hopefully just one) matching the instrument name
			Dim Cn As New SqlConnection(m_connection_str)
			Dim Da As New SqlDataAdapter(SQL, Cn)
			Dim Ds As DataSet = New DataSet

			Try
				Da.Fill(Ds)
			Catch ex As Exception
				m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Filling data adapter, " & ex.Message, ILogger.logMsgType.logError, True)
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
			End Try

			Dim table As DataTable
			For Each table In Ds.Tables
				Dim row As DataRow
				For Each row In table.Rows
					m_capture_Type = row("Capture").ToString()
					m_source_path = row("SourcePath").ToString
					'                    m_raw_Data_Type = row("RawDataType").ToString
					Exit For
				Next row
			Next table

			If String.IsNullOrEmpty(m_capture_Type) OrElse String.IsNullOrEmpty(m_source_path) Then
				m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Error retrieving source path and capture type for instrument '" & insName & "': no rows returned from V_Instrument_List_Export", ILogger.logMsgType.logError, True)
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
			End If

			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE

		Catch Err As Exception
			m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Error retrieving source path and capture type for instrument '" & insName & "': " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
		End Try

	End Function

	Private Function PerformValidation() As IXMLValidateStatus.XmlValidateStatus
		Dim m_Connected As Boolean = False
		Dim Pwd As String
		Dim RawFName As String = String.Empty
		Dim resType As RawDSTypes
		Dim dtFileModDate As DateTime
		Dim dtRunFinishWithTolerance As DateTime

		Dim strValue As String
		Dim intTimeValToleranceMinutes As Integer

		Dim currentTask As String = String.Empty

		Try

			If String.IsNullOrEmpty(m_capture_Type) OrElse String.IsNullOrEmpty(m_source_path) Then
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
			End If
			'It is a bionet location so establish a connection
			If m_capture_Type = "secfso" Then
				Dim m_UserName = m_mgrParams.GetParam("bionetuser")
				Dim m_Pwd = m_mgrParams.GetParam("bionetpwd")

				Dim currentTaskBase = "Connecting to " & m_source_path & " using secfso, user " & m_UserName &
					   ", and encoded password " & m_Pwd

				currentTask = currentTaskBase & "; Decoding password"
				Pwd = DecodePassword(m_Pwd)

				currentTask = currentTaskBase & "; Instantiating ShareConnector"
				m_ShareConnector = New ShareConnector(m_UserName, Pwd)
				m_ShareConnector.Share = m_source_path

				currentTask = currentTaskBase & "; Connecting using ShareConnector"
				If m_ShareConnector.Connect Then
					m_Connected = True
				Else
					currentTask = currentTaskBase & "; Error connecting"

					m_logger.PostEntry(
					 "Error " & m_ShareConnector.ErrorMessage & " connecting to " & m_source_path & " as user " & m_UserName &
					 " using 'secfso'", ILogger.logMsgType.logError, True)

					If m_ShareConnector.ErrorMessage = "1326" Then
						m_logger.PostEntry("You likely need to change the Capture_Method from secfso to fso; use the following query: ",
							   ILogger.logMsgType.logError, True)
					ElseIf m_ShareConnector.ErrorMessage = "53" Then
						m_logger.PostEntry("The password may need to be reset; diagnose things further using the following query: ",
							   ILogger.logMsgType.logError, True)
					ElseIf m_ShareConnector.ErrorMessage = "1219" OrElse m_ShareConnector.ErrorMessage = "1203" Then
						' Likely had error "An unexpected network error occurred" while validating the Dataset specified by the XML file
						' Need to completely exit the manager
						Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR
					Else
						m_logger.PostEntry("You can diagnose the problem using this query: ", ILogger.logMsgType.logError, True)
					End If

					m_logger.PostEntry(
					 "SELECT Inst.IN_name, SP.SP_path_ID, SP.SP_path, SP.SP_machine_name, SP.SP_vol_name_client, SP.SP_vol_name_server, SP.SP_function, Inst.IN_capture_method FROM T_Storage_Path SP INNER JOIN T_Instrument_Name Inst ON SP.SP_instrument_name = Inst.IN_name AND SP.SP_path_ID = Inst.IN_source_path_ID WHERE IN_Name = '" &
					 m_ins_Name & "'", ILogger.logMsgType.logError, True)

					Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
				End If
			End If

			' Make sure m_SleepInterval isn't too large
			If m_SleepInterval > 900 Then
				m_logger.PostEntry("Sleep interval of " & m_SleepInterval & " seconds is too large; decreasing to 900 seconds",
					   ILogger.logMsgType.logWarning, True)
				m_SleepInterval = 900
			End If

			'Determine Raw Dataset type (only should be looking for "dot_raw_files" from earlier check)
			currentTask = "Determining dataset type for " & m_dataset_Name & " at " & m_source_path
			resType = GetRawDSType(m_source_path, m_dataset_Name, RawFName)

			currentTask = "Validating operator name " & m_operator_PRN & "for " & m_dataset_Name & " at " & m_source_path
			SetOperatorName()

			Select Case resType

				Case RawDSTypes.None 'No raw dataset file or folder found
					currentTask = "Dataset not found at " & m_source_path

					m_dataset_Path = Path.Combine(m_source_path, m_dataset_Name)

					'Disconnect from BioNet if necessary
					If m_Connected Then
						currentTask = "Dataset not found; disconnecting from " & m_source_path
						DisconnectShare(m_ShareConnector, m_Connected)
					End If

					Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA

				Case RawDSTypes.File 'Dataset file found
					'Check the file size
					currentTask = "Dataset found at " & m_source_path & "; verifying file size is constant"
					m_dataset_Path = Path.Combine(m_source_path, RawFName)

					Dim blnLogonFailure As Boolean = False

					If Not VerifyConstantFileSize(m_dataset_Path, m_SleepInterval, blnLogonFailure) Then

						If Not blnLogonFailure Then
							m_logger.PostEntry(
							 "Dataset '" & m_dataset_Name & "' not ready (file size changed over " & m_SleepInterval & " seconds)",
							 ILogger.logMsgType.logWarning, True)
						End If

						If m_Connected Then
							currentTask = "Dataset size changed; disconnecting from " & m_source_path
							DisconnectShare(m_ShareConnector, m_Connected)
						End If

						If blnLogonFailure Then
							Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE
						Else
							Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED
						End If

					End If

					currentTask = "Dataset found at " & m_source_path & " and is unchanged"
					If m_run_Finish_Utc <> New DateTime(1960, 1, 1) Then
						currentTask &= "; validating file date vs. Run_Finish listed in XML trigger file (" & CStr(m_run_Finish_Utc) &
							  ")"

						dtFileModDate = File.GetLastWriteTimeUtc(m_dataset_Path)

						strValue = m_mgrParams.GetParam("timevalidationtolerance")
						If Not Integer.TryParse(strValue, intTimeValToleranceMinutes) Then
							intTimeValToleranceMinutes = 800
						End If
						dtRunFinishWithTolerance = m_run_Finish_Utc.AddMinutes(intTimeValToleranceMinutes)

						If dtFileModDate <= dtRunFinishWithTolerance Then
							Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SUCCESS
						Else
							m_logger.PostEntry(
							 "Time validation error for " & m_dataset_Name & ": file modification date (UTC): " & CStr(dtFileModDate) &
							 " vs. Run Finish UTC date: " & CStr(dtRunFinishWithTolerance) & " (includes " & intTimeValToleranceMinutes &
							 " minute tolerance)", ILogger.logMsgType.logError, False)
							Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED
						End If
					End If

				Case RawDSTypes.FolderExt, RawDSTypes.FolderNoExt 'Dataset found in a folder with an extension
					'Verify the folder size is constant
					currentTask = "Dataset folder found at " & m_source_path & "; verifying folder size is constant"
					m_dataset_Path = Path.Combine(m_source_path, RawFName)
					currentTask &= " for " & m_dataset_Path

					If Not VerifyConstantFolderSize(m_dataset_Path, m_SleepInterval) Then
						m_logger.PostEntry(
						 "Dataset '" & m_dataset_Name & "' not ready (folder size changed over " & m_SleepInterval & " seconds)",
						 ILogger.logMsgType.logWarning, True)

						If m_Connected Then
							currentTask = "Dataset folder size changed; disconnecting from " & m_source_path
							DisconnectShare(m_ShareConnector, m_Connected)
						End If

						Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED
					End If

				Case Else
					m_logger.PostEntry("Invalid dataset type for " & m_dataset_Name & ": " & resType.ToString,
						   ILogger.logMsgType.logError, False)
					If m_Connected Then
						currentTask = "Invalid dataset type; disconnecting from " & m_source_path
						DisconnectShare(m_ShareConnector, m_Connected)
					End If

					Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA
			End Select

			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SUCCESS

		Catch Err As Exception
			m_logger.PostEntry("clsXMLTimeValidation.GetInstrumentName(), Error reading XML File, current task: " & currentTask & "; " & Err.Message, ILogger.logMsgType.logError, True)

			If Err.Message.Contains("unknown user name or bad password") Then
				' Example message: Error accessing '\\VOrbi05.bionet\ProteomicsData\QC_Shew_11_02_pt5_d2_1Apr12_Earth_12-03-14.raw': Logon failure: unknown user name or bad password
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE

			ElseIf Err.Message.Contains("Access to the path") AndAlso Err.Message.Contains("is denied") Then
				' Example message: Access to the path '\\exact01.bionet\ProteomicsData\Alz_Cap_Test_14_31Mar12_Roc_12-03-16.raw' is denied.
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE

			Else
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
			End If

		End Try

	End Function

	Protected Function DecodePassword(ByVal EnPwd As String) As String

		'Decrypts password received from ini file
		' Password was created by alternately subtracting or adding 1 to the ASCII value of each character

		Dim CharCode As Byte
		Dim TempStr As String
		Dim Indx As Integer

		TempStr = String.Empty

		Indx = 1
		Do While Indx <= Len(EnPwd)
			CharCode = CByte(Asc(Mid(EnPwd, Indx, 1)))
			If Indx Mod 2 = 0 Then
				CharCode = CharCode - CByte(1)
			Else
				CharCode = CharCode + CByte(1)
			End If
			TempStr = TempStr & Chr(CharCode)
			Indx = Indx + 1
			Application.DoEvents()
		Loop

		Return TempStr

	End Function


	Protected Function GetRawDSType(ByVal InstFolder As String, ByVal DSName As String, ByRef MyName As String) As RawDSTypes

		'Determines if raw dataset exists as a single file, folder with same name as dataset, or 
		'	folder with dataset name + extension. Returns enum specifying what was found and MyName
		' containing full name of file or folder

		Dim MyInfo() As String

		'Verify instrument transfer folder exists
		If Not Directory.Exists(InstFolder) Then
			MyName = String.Empty
			Return RawDSTypes.None
		End If

		'Check for a file with specified name
		MyInfo = Directory.GetFiles(InstFolder)
		For Each TestFile As String In MyInfo
			If Path.GetFileNameWithoutExtension(TestFile).ToLower = DSName.ToLower Then
				MyName = Path.GetFileName(TestFile)
				Return RawDSTypes.File
			End If
		Next

		'Check for a folder with specified name
		MyInfo = Directory.GetDirectories(InstFolder)
		For Each TestFolder As String In MyInfo
			'Using Path.GetFileNameWithoutExtension on folders is cheezy, but it works. I did this
			'	because the Path class methods that deal with directories ignore the possibilty there
			'	might be an extension. Apparently when sending in a string, Path can't tell a file from
			'	a directory
			If Path.GetFileNameWithoutExtension(TestFolder).ToLower = DSName.ToLower Then
				If Path.GetExtension(TestFolder).Length = 0 Then
					'Found a directory that has no extension
					MyName = Path.GetFileName(TestFolder)
					Return RawDSTypes.FolderNoExt
				Else
					'Directory name has an extension
					MyName = Path.GetFileName(TestFolder)
					Return RawDSTypes.FolderExt
				End If
			End If
		Next

		'If we got to here, then the raw dataset wasn't found, so there was a problem
		MyName = String.Empty
		Return RawDSTypes.None

	End Function

	Protected Function ValidateFolderPath(ByVal InpPath As String) As Boolean
		'Verifies that the folder given by input path exists

		If Directory.Exists(InpPath) Then
			ValidateFolderPath = True
		Else
			ValidateFolderPath = False
		End If

	End Function

	Protected Sub DisconnectShare(ByRef MyConn As ShareConnector, ByRef ConnState As Boolean)

		'Disconnects a shared drive
		MyConn.Disconnect()
		ConnState = False
	End Sub


	Protected Function VerifyConstantFolderSize(ByVal FolderName As String, ByVal SleepIntervalSeconds As Integer) As Boolean

		'Determines if the size of a folder changes over specified time interval
		Dim InitialFolderSize As Long
		Dim FinalFolderSize As Long

		' Sleep interval should be no more than 15 minutes (900 seconds)
		If SleepIntervalSeconds > 900 Then SleepIntervalSeconds = 900
		If SleepIntervalSeconds < 1 Then SleepIntervalSeconds = 1

		'Get the initial size of the folder
		InitialFolderSize = m_FileTools.GetDirectorySize(FolderName)

		'Wait for specified sleep interval
		Thread.Sleep(SleepIntervalSeconds * 1000)		'Delay for specified interval

		'Get the final size of the folder and compare
		FinalFolderSize = m_FileTools.GetDirectorySize(FolderName)
		If FinalFolderSize = InitialFolderSize Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Function VerifyConstantFileSize(ByVal FileName As String, ByVal SleepIntervalSeconds As Integer, ByRef blnLogonFailure As Boolean) As Boolean

		'Determines if the size of a file changes over specified time interval
		Dim Fi As FileInfo
		Dim InitialFileSize As Long
		Dim FinalFileSize As Long

		blnLogonFailure = False

		' Sleep interval should be no more than 15 minutes (900 seconds)
		If SleepIntervalSeconds > 900 Then SleepIntervalSeconds = 900
		If SleepIntervalSeconds < 1 Then SleepIntervalSeconds = 1

		Try
			'Get the initial size of the folder
			Fi = New FileInfo(FileName)
			InitialFileSize = Fi.Length

			'Wait for specified sleep interval
			Thread.Sleep(SleepIntervalSeconds * 1000)		'Delay for specified interval

			'Get the final size of the file and compare
			Fi.Refresh()
			FinalFileSize = Fi.Length
			If FinalFileSize = InitialFileSize Then
				Return True
			Else
				Return False
			End If

		Catch ex As Exception
			m_logger.PostEntry("Error accessing '" & FileName & "': " & ex.Message, ILogger.logMsgType.logWarning, True)

			' Check for "Logon failure: unknown user name or bad password."

			If ex.Message.Contains("unknown user name or bad password") Then
				' This error occasionally occurs when monitoring a .UIMF file on an IMS instrument
				' We'll treat this as an indicator that the file size is not constant					
			Else
				Throw
			End If
		End Try

		Return False

	End Function

	Private Function SetOperatorName() As Boolean

		Dim strOperatorName As String = String.Empty
		Dim strOperatorEmail As String = String.Empty
		Dim strPRN As String = String.Empty

		Dim intUserCountMatched As Integer = 0
		Dim blnSuccess As Boolean
		Dim strLogMsg As String

		Try
			'Requests additional task parameters from database and adds them to the m_taskParams string dictionary
			Dim SQL As String
			SQL = "  SELECT U_email, U_Name, U_PRN "
			SQL += " FROM dbo.T_Users "
			SQL += " WHERE U_PRN = '" + m_operator_PRN + "'"
			SQL += " ORDER BY ID desc"

			blnSuccess = LookupOperatorName(SQL, strOperatorName, strOperatorEmail, strPRN, intUserCountMatched)

			If blnSuccess AndAlso Not String.IsNullOrEmpty(strOperatorName) Then
				m_operator_Name = strOperatorName
				m_operator_Email = strOperatorEmail
				Return True
			End If

			' m_operator_PRN may contain the person's name instead of their PRN; check for this
			' In other words, m_operator_PRN may be "Baker, Erin M" instead of "D3P347"

			Dim strQueryName As String = String.Copy(m_operator_PRN)
			If strQueryName.IndexOf("("c) > 0 Then
				' Name likely is something like: Baker, Erin M (D3P347)
				' Truncate any text after the parenthesis
				strQueryName = strQueryName.Substring(0, strQueryName.IndexOf("("c)).Trim()
			End If

			SQL = "  SELECT U_email, U_Name, U_PRN "
			SQL += " FROM dbo.T_Users "
			SQL += " WHERE U_Name LIKE '" + strQueryName + "%'"
			SQL += " ORDER BY ID desc"

			blnSuccess = LookupOperatorName(SQL, strOperatorName, strOperatorEmail, strPRN, intUserCountMatched)

			If blnSuccess AndAlso Not String.IsNullOrEmpty(strOperatorName) Then
				If intUserCountMatched = 1 Then
					' We matched a single user using strQueryName
					' Update the operator name, e-mail, and PRN
					m_operator_Name = strOperatorName
					m_operator_Email = strOperatorEmail
					m_operator_PRN = strPRN
					Return True
				ElseIf intUserCountMatched > 1 Then
					strLogMsg = "clsXMLTimeValidation.SetOperatorName: Ambiguous match found for '" & strOperatorName & "' in T_Users.U_PRN; will e-mail '" & strOperatorEmail & "'"
					m_logger.PostEntry(strLogMsg, ILogger.logMsgType.logWarning, True)

					m_operator_Name = "Ambiguous match found for operator (" + strOperatorName + "); use network login instead, e.g. D3E154"

					' Update operator e-mail anwyway; that way at least somebody will get the e-mail
					m_operator_Email = strOperatorEmail
					Return False
				End If
			End If

			strLogMsg = "clsXMLTimeValidation.SetOperatorName: Operator not found in T_Users.U_PRN: " & m_operator_PRN
			m_logger.PostEntry(strLogMsg, ILogger.logMsgType.logWarning, True)

			m_operator_Name = "Operator " + m_operator_PRN + " not found in T_Users; should be network login name, e.g. D3E154"
			Return False

		Catch Err As Exception
			strLogMsg = "clsXMLTimeValidation.RetrieveOperatorName(), Error retrieving Operator Name, " & Err.Message
			m_logger.PostEntry(strLogMsg, ILogger.logMsgType.logError, True)
			Return False
		End Try

	End Function

	Private Function LookupOperatorName(ByVal SQL As String, ByRef strOperatorName As String, ByRef strOperatorEmail As String, ByRef strPRN As String, ByRef intUserCountMatched As Integer) As Boolean

		'Get a list of all records in database (hopefully just one) matching the user PRN
		Dim Cn As New SqlConnection(m_connection_str)
		Dim Da As New SqlDataAdapter(SQL, Cn)
		Dim Ds As DataSet = New DataSet
		Dim blnSuccess As Boolean

		blnSuccess = False
		intUserCountMatched = 0

		Try
			Da.Fill(Ds)
		Catch ex As Exception
			m_logger.PostEntry("clsXMLTimeValidation.RetrieveOperatorName(), Filling data adapter, " & ex.Message, ILogger.logMsgType.logError, True)
			Return False
		End Try

		strOperatorEmail = String.Empty
		strOperatorName = String.Empty
		Dim table As DataTable
		For Each table In Ds.Tables
			intUserCountMatched = table.Rows.Count

			Dim row As DataRow
			For Each row In table.Rows
				strOperatorEmail = row("U_email").ToString()
				strOperatorName = row("U_Name").ToString()
				strPRN = row("U_PRN").ToString()
				blnSuccess = True
				Exit For
			Next row
			Exit For
		Next table

		Return blnSuccess

	End Function

End Class
