Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports DataImportManager.MgrSettings
Imports System.Data.SqlClient
Imports System.IO
Imports DataImportManager.clsGlobal
Imports System.Windows.Forms


Public Class clsXMLTimeValidation

	Inherits clsDBTask
	Public mp_db_err_msg As String
	Private m_ins_Name As String = String.Empty
	Private m_dataset_Name As String = String.Empty
	Private m_run_Finish_Utc As Date = CDate("1/1/1960")
	Private m_capture_Type As String = String.Empty
	Private m_source_path As String = String.Empty
	Public m_operator_PRN As String = String.Empty
	Public m_operator_Email As String = String.Empty
	Public m_operator_Name As String = String.Empty
	Public m_dataset_Path As String = String.Empty
	Private m_UseBioNet As Boolean = False
	Protected m_ShareConnector As ShareConnector
	Protected m_SleepInterval As Integer = 30

#Region "Enums"
	Protected Enum RawDSTypes
		None
		File
		FolderNoExt
		FolderExt
	End Enum
#End Region

	Public ReadOnly Property DatasetName() As String
		Get
			If m_dataset_Name Is Nothing Then
				Return String.Empty
			Else
				Return m_dataset_Name
			End If

		End Get
	End Property

	Public ReadOnly Property SourcePath() As String
		Get
			If m_source_path Is Nothing Then
				Return String.Empty
			Else
				Return m_source_path
			End If
		End Get
	End Property

	Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger)
		MyBase.New(mgrParams, logger)
	End Sub

	Public Function ValidateXMLFile(ByVal xmlFile As String) As IXMLValidateStatus.XmlValidateStatus
		Dim rslt As IXMLValidateStatus.XmlValidateStatus

		m_connection_str = m_mgrParams.GetParam("ConnectionString")
		Dim m_xmlFileValid As Boolean
		m_xmlFileValid = False

		Try
			OpenConnection()
		Catch Err As System.Exception
			m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), error opening connection, " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED
		End Try

		Try
			rslt = GetXMLParameters(xmlFile)
			If rslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
				rslt = SetDbInstrumentParameters(m_ins_Name)
				If rslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
					rslt = PerformValidation()
				Else
					Return rslt
				End If
			Else
				Return rslt
			End If

		Catch Err As System.Exception
			m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), Error running ValidateXMLFile, " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
		End Try

		Try
			CLoseConnection()
		Catch Err As System.Exception
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

		Catch Err As System.Exception
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

		Catch Err As System.Exception
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

		Catch Err As System.Exception
			m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Error retrieving source path and capture type for instrument '" & insName & "': " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
		End Try

	End Function

	Private Function PerformValidation() As IXMLValidateStatus.XmlValidateStatus
		Dim m_Connected As Boolean = False
		Dim m_UserName As String = String.Empty
		Dim m_Pwd As String = String.Empty
		Dim Pwd As String
		Dim fileModDate As DateTime
		Dim RawFName As String = String.Empty
		Dim resType As RawDSTypes
		Dim runFinishwTolerance As Date
		Dim timevaltolerance As Integer

		Try

			If Not String.IsNullOrEmpty(m_capture_Type) AndAlso Not String.IsNullOrEmpty(m_source_path) Then
				'it is a bionet location so establish a connection
				If m_capture_Type = "secfso" Then
					m_UserName = m_mgrParams.GetParam("bionetuser")
					m_Pwd = m_mgrParams.GetParam("bionetpwd")
					Pwd = DecodePassword(m_Pwd)
					m_ShareConnector = New ShareConnector(m_UserName, Pwd)
					m_ShareConnector.Share = m_source_path
					If m_ShareConnector.Connect Then
						m_Connected = True
					Else
						m_logger.PostEntry("Error " & m_ShareConnector.ErrorMessage & " connecting to " & m_source_path & " as user " & m_UserName & " using 'secfso'", _
						 ILogger.logMsgType.logError, True)

						If m_ShareConnector.ErrorMessage = "1326" Then
							m_logger.PostEntry("You likely need to change the Capture_Method from secfso to fso; use the following query: ", _
							 ILogger.logMsgType.logError, True)
						ElseIf m_ShareConnector.ErrorMessage = "53" Then
							m_logger.PostEntry("The password may need to be reset; diagnose things further using the following query: ", _
							 ILogger.logMsgType.logError, True)
						Else
							m_logger.PostEntry("You can diagnose the problem using this query: ", _
							 ILogger.logMsgType.logError, True)
						End If

						m_logger.PostEntry("SELECT Inst.IN_name, SP.SP_path_ID, SP.SP_path, SP.SP_machine_name, SP.SP_vol_name_client, SP.SP_vol_name_server, SP.SP_function, Inst.IN_capture_method FROM T_Storage_Path SP INNER JOIN T_Instrument_Name Inst ON SP.SP_instrument_name = Inst.IN_name AND SP.SP_path_ID = Inst.IN_source_path_ID WHERE IN_Name = '" & m_ins_Name & "'", _
						 ILogger.logMsgType.logError, True)

						Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
					End If
				End If

				' Make sure m_SleepInterval isn't too large
				If m_SleepInterval > 900 Then
					m_logger.PostEntry("Sleep interval of " & m_SleepInterval & " seconds is too large; decreasing to 900 seconds", ILogger.logMsgType.logWarning, True)
					m_SleepInterval = 900
				End If

				'Determine Raw Dataset type (only should be looking for "dot_raw_files" from earlier check)
				resType = GetRawDSType(m_source_path, m_dataset_Name, RawFName)
				SetOperatorName()
				Select Case resType

					Case RawDSTypes.None		  'No raw dataset file or folder found
						m_logger.PostEntry("Dataset " & m_dataset_Name & " not found at " & m_source_path, ILogger.logMsgType.logError, True)
						'Disconnect from BioNet if necessary
						m_dataset_Path = Path.Combine(m_source_path, RawFName)
						If m_Connected Then DisconnectShare(m_ShareConnector, m_Connected)
						Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA

					Case RawDSTypes.File		  'Dataset file found
						'Check the file size
						If Not VerifyConstantFileSize(Path.Combine(m_source_path, RawFName), m_SleepInterval) Then
							m_logger.PostEntry("Dataset '" & m_dataset_Name & "' not ready (file size changed over " & m_SleepInterval & " seconds)", ILogger.logMsgType.logWarning, True)
							If m_Connected Then DisconnectShare(m_ShareConnector, m_Connected)
							m_dataset_Path = Path.Combine(m_source_path, RawFName)
							Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA
						End If
						If m_run_Finish_Utc <> CDate("1/1/1960") Then
							fileModDate = File.GetLastWriteTimeUtc(Path.Combine(m_source_path, RawFName))
							timevaltolerance = m_mgrParams.GetParam("timevalidationtolerance")
							runFinishwTolerance = m_run_Finish_Utc.AddMinutes(timevaltolerance)
							If fileModDate <= runFinishwTolerance Then
								Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SUCCESS
							Else
								m_dataset_Path = Path.Combine(m_source_path, RawFName)
								m_logger.PostEntry("Time validation error.  Dataset file modification date: " & CStr(fileModDate), ILogger.logMsgType.logError, False)
								m_logger.PostEntry("Time validation error.  Run Finish UTC date with tolerance: " & CStr(runFinishwTolerance), ILogger.logMsgType.logError, False)
								Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED
							End If
						End If

					Case RawDSTypes.FolderExt		  'Dataset found in a folder with an extension
						'Verify the folder size is constant
						If Not VerifyConstantFolderSize(Path.Combine(m_source_path, RawFName), m_SleepInterval) Then
							m_logger.PostEntry("Dataset '" & m_dataset_Name & "' not ready (folder size changed over " & m_SleepInterval & " seconds)", ILogger.logMsgType.logWarning, True)
							If m_Connected Then DisconnectShare(m_ShareConnector, m_Connected)
							m_dataset_Path = Path.Combine(m_source_path, RawFName)
							Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA
						End If

					Case RawDSTypes.FolderNoExt
						'Verify the folder size is constant
						If Not VerifyConstantFolderSize(Path.Combine(m_source_path, RawFName), m_SleepInterval) Then
							m_logger.PostEntry("Dataset '" & m_dataset_Name & "' not ready (folder size changed over " & m_SleepInterval & " seconds)", ILogger.logMsgType.logWarning, True)
							If m_Connected Then DisconnectShare(m_ShareConnector, m_Connected)
							m_dataset_Path = Path.Combine(m_source_path, RawFName)
							Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA
						End If
					Case Else
						m_logger.PostEntry("Invalid dataset type found: " & resType.ToString, ILogger.logMsgType.logError, False)
						If m_Connected Then DisconnectShare(m_ShareConnector, m_Connected)
						Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA
				End Select

				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SUCCESS
			Else
				Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
			End If

			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_CHECK

		Catch Err As System.Exception
			m_logger.PostEntry("clsXMLTimeValidation.GetInstrumentName(), Error reading XML File, " & Err.Message, ILogger.logMsgType.logError, True)
			Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
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


	Protected Function VerifyConstantFolderSize(ByVal FolderName As String, ByVal SleepInt As Integer) As Boolean

		'Determines if the size of a folder changes over specified time interval
		Dim InitialFolderSize As Long
		Dim FinalFolderSize As Long

		'Verify maximum sleep interval
		If (CLng(SleepInt) * 1000) > [Integer].MaxValue Then
			SleepInt = CInt([Integer].MaxValue / 1000)
		End If

		'Get the initial size of the folder
		InitialFolderSize = GetDirectorySize(FolderName)

		'Wait for specified sleep interval
		System.Threading.Thread.Sleep(SleepInt * 1000)		'Delay for specified interval

		'Get the final size of the folder and compare
		FinalFolderSize = GetDirectorySize(FolderName)
		If FinalFolderSize = InitialFolderSize Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Function VerifyConstantFileSize(ByVal FileName As String, ByVal SleepInt As Integer) As Boolean

		'Determines if the size of a file changes over specified time interval
		Dim Fi As FileInfo
		Dim InitialFileSize As Long
		Dim FinalFileSize As Long

		If SleepInt > 900 Then
			' Sleep interval should be no more than 15 minutes
			SleepInt = 900
		End If

		'Get the initial size of the folder
		Fi = New FileInfo(FileName)
		InitialFileSize = Fi.Length

		'Wait for specified sleep interval
		System.Threading.Thread.Sleep(SleepInt * 1000)		'Delay for specified interval

		'Get the final size of the file and compare
		Fi.Refresh()
		FinalFileSize = Fi.Length
		If FinalFileSize = InitialFileSize Then
			Return True
		Else
			Return False
		End If

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

			strLogMsg = "clsXMLTimeValidation.SetOperatorName: '" & m_operator_PRN & "' not found in T_Users.U_PRN; will try '" & strQueryName & "'"
			m_logger.PostEntry(strLogMsg, ILogger.logMsgType.logWarning, True)

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

			m_operator_Name = "Operator not found (" + m_operator_PRN + "); should be network login name, e.g. D3E154"
			Return False

		Catch Err As System.Exception
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
