Imports System.Collections.Generic
Imports PRISM.Logging
Imports PRISM.Files
Imports System.Data.SqlClient
Imports System.IO
Imports DataImportManager.clsGlobal
Imports System.Windows.Forms
Imports System.Threading
Imports System.Reflection


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
            If TraceMode Then clsMainProcess.ShowTraceMessage("Reading " & xmlFilePath)
            rslt = GetXMLParameters(xmlFilePath)
        Catch ex As Exception
            Dim errMsg = "clsXMLTimeValidation.ValidateXMLFile(), Error reading the XML file " & Path.GetFileName(xmlFilePath) & ": " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)
            m_logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
        End Try

        If rslt <> IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
            Return rslt
        ElseIf m_InstrumentsToSkip.ContainsKey(m_ins_Name) Then
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SKIP_INSTRUMENT
        End If

        Try
            OpenConnection()
        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), error opening connection, " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED
        End Try

        Try

            rslt = SetDbInstrumentParameters(m_ins_Name)
            If rslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
                rslt = PerformValidation()
            Else
                Return rslt
            End If

        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), Error calling PerformValidation, " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
        End Try

        Try
            CLoseConnection()
        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), Error closing connection, " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
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
            xmlDataSet.ReadXml(xmlStringReader)  'Everything must be OK if we got to here
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
                m_logger.PostEntry("clsXMLTimeValidation.GetXMLParameters(), The instrument name was blank.", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
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

        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.GetXMLParameters(), Error reading XML File, " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
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
                Dim statusMessage = "clsXMLTimeValidation.InstrumentWaitDelay(), The dataset import is being delayed for XML File: " + xmlFilename
                If TraceMode Then ShowTraceMessage(statusMessage)
                m_logger.PostEntry(statusMessage, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
                Return IXMLValidateStatus.XmlValidateStatus.XML_WAIT_FOR_FILES
            End If

            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE

        Catch ex As Exception
            Dim errMsg = "clsXMLTimeValidation.InstrumentWaitDelay(), Error determining wait delay, " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)
            m_logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
        End Try

    End Function

    ''' <summary>
    ''' Query to get the instrument data from the database and then iterate through the dataset to retrieve the capture type and source path
    ''' </summary>
    ''' <param name="insName"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function SetDbInstrumentParameters(ByVal insName As String) As IXMLValidateStatus.XmlValidateStatus

        Try
            'Requests additional task parameters from database and adds them to the m_taskParams string dictionary
            Dim sqlQuery As String
            sqlQuery = "SELECT Name, Class, RawDataType, Capture, SourcePath " &
               " FROM dbo.V_Instrument_List_Export " &
               " WHERE Name = '" + insName + "'" &
               " ORDER BY Name "

            'Get a list of all records in database (hopefully just one) matching the instrument name
            Dim Cn As New SqlConnection(m_connection_str)
            Dim Da As New SqlDataAdapter(sqlQuery, Cn)
            Dim Ds As DataSet = New DataSet

            If TraceMode Then ShowTraceMessage("Running query on database " & Cn.Database & ": " & sqlQuery)
            Try
                Da.Fill(Ds)
            Catch ex As Exception
                m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Filling data adapter, " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
            End Try

            Dim table As DataTable
            For Each table In Ds.Tables
                Dim row As DataRow
                For Each row In table.Rows
                    m_capture_Type = row("Capture").ToString()
                    m_source_path = row("SourcePath").ToString
                    Exit For
                Next row
            Next table

            If String.IsNullOrEmpty(m_capture_Type) OrElse String.IsNullOrEmpty(m_source_path) Then
                m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Error retrieving source path and capture type for instrument '" & insName & "': no rows returned from V_Instrument_List_Export", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
            End If

            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE

        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Error retrieving source path and capture type for instrument '" & insName & "': " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
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
        Dim statusMsg As String = String.Empty

        Try
           
            If String.IsNullOrEmpty(m_capture_Type) OrElse String.IsNullOrEmpty(m_source_path) Then
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
            End If

            If m_capture_Type = "secfso" Then
                ' Make sure m_source_path is not of the form \\proto-2 because if that is the case, then m_capture_type should be "fso"
                Dim reProtoServer = New System.Text.RegularExpressions.Regex("\\\\proto-\d+\\", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)

                If reProtoServer.IsMatch(m_source_path) Then
                    ' Auto-change m_capture_Type to "fso", and log an error in the database
                    m_capture_Type = "fso"
                    Dim errMsg = "Instrument " & m_ins_Name & " is configured to use 'secfso' yet its source folder is " & m_source_path &
                        ", which appears to be a domain path; auto-changing the capture_method to 'fso' for now, " &
                        "but the configuration in the database should be updated (see table T_Instrument_Name)"
                    If TraceMode Then
                        Console.WriteLine(" - - - - - - - - ")
                        ShowTraceMessage("ERROR: " & errMsg)
                        Console.WriteLine(" - - - - - - - - ")
                    End If
                    m_logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_DATABASE)
                End If
            End If

            If m_capture_Type = "secfso" AndAlso Not Environment.MachineName.ToLower().StartsWith("monroe") Then
                ' Source folder is on bionet; establish a connection

                Dim m_UserName = m_mgrParams.GetParam("bionetuser")
                Dim m_Pwd = m_mgrParams.GetParam("bionetpwd")

                If Not m_UserName.Contains("\"c) Then
                    ' Prepend this computer's name to the username
                    m_UserName = System.Environment.MachineName & "\" & m_UserName
                End If

                Dim currentTaskBase = "Connecting to " & m_source_path & " using secfso, user " & m_UserName &
                 ", and encoded password " & m_Pwd

                currentTask = currentTaskBase & "; Decoding password"
                If TraceMode Then ShowTraceMessage(currentTask)
                Pwd = DecodePassword(m_Pwd)

                currentTask = currentTaskBase & "; Instantiating ShareConnector"
                If TraceMode Then ShowTraceMessage(currentTask)
                m_ShareConnector = New ShareConnector(m_UserName, Pwd)
                m_ShareConnector.Share = m_source_path

                currentTask = currentTaskBase & "; Connecting using ShareConnector"
                If TraceMode Then ShowTraceMessage(currentTask)

                If m_ShareConnector.Connect() Then
                    m_Connected = True
                Else
                    currentTask = currentTaskBase & "; Error connecting"

                    Dim errorMessage = "Error " & m_ShareConnector.ErrorMessage & " connecting to " & m_source_path & " as user " & m_UserName
                    If TraceMode Then ShowTraceMessage(errorMessage)
                    errorMessage &= " using 'secfso'" + "; error code " + m_ShareConnector.ErrorMessage

                    m_logger.PostEntry(errorMessage, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)

                    If m_ShareConnector.ErrorMessage = "1326" Then
                        statusMsg = "You likely need to change the Capture_Method from secfso to fso; use the following query: "
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)

                    ElseIf m_ShareConnector.ErrorMessage = "53" Then
                        statusMsg = "The password may need to be reset; diagnose things further using the following query: "
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)

                    ElseIf m_ShareConnector.ErrorMessage = "1219" OrElse m_ShareConnector.ErrorMessage = "1203" Then
                        statusMsg = "Likely had error 'An unexpected network error occurred' while validating the Dataset specified by the XML file (ErrorMessage=" & m_ShareConnector.ErrorMessage & ")"
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        ' Likely had error "An unexpected network error occurred" while validating the Dataset specified by the XML file
                        ' Need to completely exit the manager
                        Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR

                    Else
                        statusMsg = "You can diagnose the problem using this query: "
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                    End If

                    statusMsg = "SELECT Inst.IN_name, SP.SP_path_ID, SP.SP_path, SP.SP_machine_name, SP.SP_vol_name_client, SP.SP_vol_name_server, SP.SP_function, Inst.IN_capture_method " &
                                "FROM T_Storage_Path SP INNER JOIN T_Instrument_Name Inst ON SP.SP_instrument_name = Inst.IN_name AND SP.SP_path_ID = Inst.IN_source_path_ID " &
                                "WHERE IN_Name = '" & m_ins_Name & "'"

                    If TraceMode Then ShowTraceMessage(statusMsg)
                    m_logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)

                    Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
                End If
            End If

            ' Make sure m_SleepInterval isn't too large
            If m_SleepInterval > 900 Then
                m_logger.PostEntry("Sleep interval of " & m_SleepInterval & " seconds is too large; decreasing to 900 seconds",
                 ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
                m_SleepInterval = 900
            End If

            'Determine Raw Dataset type (only should be looking for "dot_raw_files" from earlier check)
            currentTask = "Determining dataset type for " & m_dataset_Name & " at " & m_source_path
            If TraceMode Then ShowTraceMessage(currentTask)
            resType = GetRawDSType(m_source_path, m_dataset_Name, RawFName)

            currentTask = "Validating operator name " & m_operator_PRN & " for " & m_dataset_Name & " at " & m_source_path
            If TraceMode Then ShowTraceMessage(currentTask)
            If Not SetOperatorName() Then
                If m_Connected Then
                    currentTask = "Operator not found; disconnecting from " & m_source_path
                    If TraceMode Then ShowTraceMessage(currentTask)
                    DisconnectShare(m_ShareConnector, m_Connected)
                End If
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_OPERATOR
            End If

            If Environment.MachineName.ToLower().StartsWith("monroe") Then
                Console.WriteLine("Skipping bionet validation because host name starts with Monroe")
            Else
                Select Case resType

                    Case RawDSTypes.None 'No raw dataset file or folder found
                        currentTask = "Dataset not found at " & m_source_path
                        If TraceMode Then ShowTraceMessage(currentTask)
                        m_dataset_Path = Path.Combine(m_source_path, m_dataset_Name)

                        'Disconnect from BioNet if necessary
                        If m_Connected Then
                            currentTask = "Dataset not found; disconnecting from " & m_source_path
                            If TraceMode Then ShowTraceMessage(currentTask)
                            DisconnectShare(m_ShareConnector, m_Connected)
                        End If

                        Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA

                    Case RawDSTypes.File 'Dataset file found
                        'Check the file size
                        currentTask = "Dataset found at " & m_source_path & "; verifying file size is constant"
                        If TraceMode Then ShowTraceMessage(currentTask)
                        m_dataset_Path = Path.Combine(m_source_path, RawFName)

                        Dim blnLogonFailure As Boolean = False

                        If Not VerifyConstantFileSize(m_dataset_Path, m_SleepInterval, blnLogonFailure) Then

                            If Not blnLogonFailure Then
                                statusMsg = "Dataset '" & m_dataset_Name & "' not ready (file size changed over " & m_SleepInterval & " seconds)"
                                If TraceMode Then ShowTraceMessage(statusMsg)
                                m_logger.PostEntry(statusMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
                            End If

                            If m_Connected Then
                                currentTask = "Dataset size changed; disconnecting from " & m_source_path
                                If TraceMode Then ShowTraceMessage(statusMsg)
                                DisconnectShare(m_ShareConnector, m_Connected)
                            End If

                            If blnLogonFailure Then
                                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE
                            Else
                                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED
                            End If

                        End If

                        currentTask = "Dataset found at " & m_source_path & " and is unchanged"
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        If m_run_Finish_Utc <> New DateTime(1960, 1, 1) Then
                            currentTask &= "; validating file date vs. Run_Finish listed in XML trigger file (" & CStr(m_run_Finish_Utc) & ")"
                            If TraceMode Then ShowTraceMessage(currentTask)

                            dtFileModDate = File.GetLastWriteTimeUtc(m_dataset_Path)

                            strValue = m_mgrParams.GetParam("timevalidationtolerance")
                            If Not Integer.TryParse(strValue, intTimeValToleranceMinutes) Then
                                intTimeValToleranceMinutes = 800
                            End If
                            dtRunFinishWithTolerance = m_run_Finish_Utc.AddMinutes(intTimeValToleranceMinutes)

                            If dtFileModDate <= dtRunFinishWithTolerance Then
                                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SUCCESS
                            Else
                                statusMsg = "Time validation error for " & m_dataset_Name & ": file modification date (UTC): " & CStr(dtFileModDate) &
                                 " vs. Run Finish UTC date: " & CStr(dtRunFinishWithTolerance) & " (includes " & intTimeValToleranceMinutes &
                                 " minute tolerance)"
                                If TraceMode Then ShowTraceMessage(statusMsg)
                                m_logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_DATABASE)
                                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED
                            End If
                        End If

                    Case RawDSTypes.FolderExt, RawDSTypes.FolderNoExt 'Dataset found in a folder with an extension
                        'Verify the folder size is constant
                        currentTask = "Dataset folder found at " & m_source_path & "; verifying folder size is constant for "
                        m_dataset_Path = Path.Combine(m_source_path, RawFName)
                        currentTask &= m_dataset_Path

                        If TraceMode Then ShowTraceMessage(currentTask)

                        If Not VerifyConstantFolderSize(m_dataset_Path, m_SleepInterval) Then
                            m_logger.PostEntry(
                             "Dataset '" & m_dataset_Name & "' not ready (folder size changed over " & m_SleepInterval & " seconds)",
                             ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)

                            If m_Connected Then
                                currentTask = "Dataset folder size changed; disconnecting from " & m_source_path
                                DisconnectShare(m_ShareConnector, m_Connected)
                            End If

                            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED
                        End If

                    Case Else
                        m_logger.PostEntry("Invalid dataset type for " & m_dataset_Name & ": " & resType.ToString,
                         ILogger.logMsgType.logError, LOG_DATABASE)
                        If m_Connected Then
                            currentTask = "Invalid dataset type; disconnecting from " & m_source_path
                            DisconnectShare(m_ShareConnector, m_Connected)
                        End If

                        Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA
                End Select

            End If

            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SUCCESS

        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.GetInstrumentName(), Error reading XML File, current task: " & currentTask & "; " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)

            If ex.Message.Contains("unknown user name or bad password") Then
                ' Example message: Error accessing '\\VOrbi05.bionet\ProteomicsData\QC_Shew_11_02_pt5_d2_1Apr12_Earth_12-03-14.raw': Logon failure: unknown user name or bad password
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE

            ElseIf ex.Message.Contains("Access to the path") AndAlso ex.Message.Contains("is denied") Then
                ' Example message: Access to the path '\\exact01.bionet\ProteomicsData\Alz_Cap_Test_14_31Mar12_Roc_12-03-16.raw' is denied.
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE

            ElseIf ex.Message.Contains("network path was not found") Then
                ' Example message: The network path was not found.
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR

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
        Dim diSourceFolder = New DirectoryInfo(InstFolder)
        If TraceMode Then ShowTraceMessage("Instantiated diSourceFolder with " & "[" & InstFolder & "]")

        If Not diSourceFolder.Exists Then
            Dim statusMsg = "Source folder not found for dataset " & DSName & ": [" & diSourceFolder.FullName & "]"
            If TraceMode Then clsMainProcess.ShowTraceMessage(statusMsg)
            m_logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
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

        If TraceMode Then ShowTraceMessage("Disconnecting from Bionet share")

        'Disconnects a shared drive
        MyConn.Disconnect()
        ConnState = False

    End Sub

    ''' <summary>
    ''' Determines if the size of a folder changes over specified time interval
    ''' </summary>
    ''' <param name="folderPath"></param>
    ''' <param name="sleepIntervalSeconds"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function VerifyConstantFolderSize(ByVal folderPath As String, ByVal sleepIntervalSeconds As Integer) As Boolean
        ' Sleep interval should be no more than 15 minutes (900 seconds)
        If sleepIntervalSeconds > 900 Then sleepIntervalSeconds = 900
        If sleepIntervalSeconds < 1 Then sleepIntervalSeconds = 1

        'Get the initial size of the folder
        Dim initialFolderSize = m_FileTools.GetDirectorySize(folderPath)

        Dim sleepIntervalMsec = sleepIntervalSeconds * 1000
        If TraceMode Then
            If sleepIntervalSeconds > 3 Then
                sleepIntervalMsec = 3000
                ShowTraceMessage("Monitoring dataset folder for 3 seconds to see if its size changes (would wait " & sleepIntervalSeconds & " seconds if PreviewMode was not enabled)")
            Else
                ShowTraceMessage("Monitoring dataset folder for " & sleepIntervalSeconds & " seconds to see if its size changes")
            End If
        End If

        'Wait for specified sleep interval
        Thread.Sleep(sleepIntervalMsec)

        'Get the final size of the folder and compare
        Dim finalFolderSize = m_FileTools.GetDirectorySize(folderPath)
        If finalFolderSize = initialFolderSize Then
            Return True
        Else
            Return False
        End If

    End Function

    ''' <summary>
    ''' Determines if the size of a file changes over specified time interval
    ''' </summary>
    ''' <param name="filePath"></param>
    ''' <param name="sleepIntervalSeconds"></param>
    ''' <param name="blnLogonFailure"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function VerifyConstantFileSize(ByVal filePath As String, ByVal sleepIntervalSeconds As Integer, ByRef blnLogonFailure As Boolean) As Boolean

        blnLogonFailure = False

        ' Sleep interval should be no more than 15 minutes (900 seconds)
        If sleepIntervalSeconds > 900 Then sleepIntervalSeconds = 900
        If sleepIntervalSeconds < 1 Then sleepIntervalSeconds = 1

        Try
            'Get the initial size of the folder
            Dim fiDatasetFile = New FileInfo(filePath)
            Dim initialFileSize = fiDatasetFile.Length

            Dim sleepIntervalMsec = sleepIntervalSeconds * 1000
            If TraceMode Then
                If sleepIntervalSeconds > 3 Then
                    sleepIntervalMsec = 3000
                    ShowTraceMessage("Monitoring dataset file for 3 seconds to see if its size changes (would wait " & sleepIntervalSeconds & " seconds if PreviewMode was not enabled)")
                Else
                    ShowTraceMessage("Monitoring dataset file for " & sleepIntervalSeconds & " seconds to see if its size changes")
                End If
            End If

            'Wait for specified sleep interval
            Thread.Sleep(sleepIntervalMsec)

            'Get the final size of the file and compare
            fiDatasetFile.Refresh()
            Dim finalFileSize = fiDatasetFile.Length
            If finalFileSize = initialFileSize Then
                Return True
            Else
                Return False
            End If

        Catch ex As Exception
            Dim errMsg = "Error accessing '" & filePath & "': " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)
            m_logger.PostEntry(errMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)

            ' Check for "Logon failure: unknown user name or bad password."

            If ex.Message.Contains("unknown user name or bad password") Then
                ' This error occasionally occurs when monitoring a .UIMF file on an IMS instrument
                ' We'll treat this as an indicator that the file size is not constant					                
                If TraceMode Then ShowTraceMessage("Error message contains 'unknown user name or bad password'; assuming this means the file size is not constant")
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
            If String.IsNullOrWhiteSpace(m_operator_PRN) Then
                strLogMsg = "clsXMLTimeValidation.SetOperatorName: Operator field is empty (should be a network login, e.g. D3E154)"
                If TraceMode Then ShowTraceMessage(strLogMsg)
                m_logger.PostEntry(strLogMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)

                m_operator_Name = strLogMsg
                Return False
            End If

            'Requests additional task parameters from database and adds them to the m_taskParams string dictionary
            Dim sqlQuery As String = "  SELECT U_email, U_Name, U_PRN " &
                                    " FROM dbo.T_Users " &
                                    " WHERE U_PRN = '" + m_operator_PRN + "'" &
                                    " ORDER BY ID desc"
            blnSuccess = LookupOperatorName(sqlQuery, strOperatorName, strOperatorEmail, strPRN, intUserCountMatched)

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

            sqlQuery = " SELECT U_email, U_Name, U_PRN " &
                       " FROM dbo.T_Users " &
                       " WHERE U_Name LIKE '" + strQueryName + "%'" &
                       " ORDER BY ID desc"

            blnSuccess = LookupOperatorName(sqlQuery, strOperatorName, strOperatorEmail, strPRN, intUserCountMatched)

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
                    m_logger.PostEntry(strLogMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)

                    m_operator_Name = "Ambiguous match found for operator (" + strOperatorName + "); use network login instead, e.g. D3E154"

                    ' Update operator e-mail anwyway; that way at least somebody will get the e-mail
                    m_operator_Email = strOperatorEmail
                    Return False
                End If
            End If


            strLogMsg = "clsXMLTimeValidation.SetOperatorName: Operator not found in T_Users.U_PRN: " & m_operator_PRN
            m_logger.PostEntry(strLogMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)

            m_operator_Name = "Operator " + m_operator_PRN + " not found in T_Users; should be network login name, e.g. D3E154"
            Return False

        Catch ex As Exception
            strLogMsg = "clsXMLTimeValidation.RetrieveOperatorName(), Error retrieving Operator Name, " & ex.Message
            m_logger.PostEntry(strLogMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
            Return False
        End Try

    End Function

    Private Function LookupOperatorName(ByVal sqlQuery As String, ByRef strOperatorName As String, ByRef strOperatorEmail As String, ByRef strPRN As String, ByRef intUserCountMatched As Integer) As Boolean

        'Get a list of all records in database (hopefully just one) matching the user PRN
        Dim Cn As New SqlConnection(m_connection_str)
        Dim Da As New SqlDataAdapter(sqlQuery, Cn)
        Dim Ds As DataSet = New DataSet
        Dim blnSuccess As Boolean

        If TraceMode Then ShowTraceMessage("Looking up operator name in " & Cn.Database & " using " & sqlQuery)
        blnSuccess = False
        intUserCountMatched = 0

        Try
            Da.Fill(Ds)
        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.RetrieveOperatorName(), Filling data adapter, " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
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

        If TraceMode Then
            If intUserCountMatched > 0 Then
                ShowTraceMessage("  Operator: " & strOperatorName)
                ShowTraceMessage("  EMail: " & strOperatorEmail)
                ShowTraceMessage("  Username: " & strPRN)
            Else
                ShowTraceMessage("  Warning: database query did not return any results")
            End If
        End If

        Return blnSuccess

    End Function

End Class
