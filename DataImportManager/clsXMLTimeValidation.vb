Imports System.Collections.Concurrent
Imports System.IO
Imports System.Threading
Imports System.Runtime.InteropServices
Imports DataImportManager.clsGlobal
Imports PRISM

Public Class clsXMLTimeValidation

#Region "Member variables"
    Private mInstrumentName As String = String.Empty
    Private mCaptureSubfolder As String = String.Empty
    Private mDatasetName As String = String.Empty
    Private mRunFinishUTC As DateTime = New DateTime(1960, 1, 1)
    Private mCaptureType As String = String.Empty
    Private mSourcePath As String = String.Empty
    Private mOperatorPRN As String = String.Empty
    Private mOperatorEmail As String = String.Empty
    Private mOperatorName As String = String.Empty
    Private mDatasetPath As String = String.Empty

    Private mErrorMessage As String = String.Empty

    Private mShareConnector As ShareConnector
    Private mSleepInterval As Integer = 30

    Private ReadOnly mDMSInfoCache As DMSInfoCache

    Private ReadOnly mProcSettings As clsProcessXmlTriggerFile.udtXmlProcSettingsType

    Private ReadOnly m_InstrumentsToSkip As ConcurrentDictionary(Of String, Integer)
    Private WithEvents m_FileTools As clsFileTools

    ' access to the logger
    Private ReadOnly m_logger As ILogger

    ' access to mgr parameters
    Private ReadOnly m_mgrParams As IMgrParams


#End Region

#Region "Properties"

    ' ReSharper disable once UnusedMember.Global
    Public ReadOnly Property CaptureSubfolder As String
        Get
            Return mCaptureSubfolder
        End Get
    End Property

    Public ReadOnly Property DatasetName() As String
        Get
            Return FixNull(mDatasetName)
        End Get
    End Property

    ''' <summary>
    ''' Source path to the dataset on the instrument
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property DatasetPath() As String
        Get
            Return FixNull(mDatasetPath)
        End Get
    End Property

    Public ReadOnly Property InstrumentName() As String
        Get
            Return mInstrumentName
        End Get
    End Property

    Public ReadOnly Property ErrorMessage As String
        Get
            Return mErrorMessage
        End Get
    End Property

    Public ReadOnly Property OperatorEMail() As String
        Get
            Return FixNull(mOperatorEmail)
        End Get
    End Property

    Public ReadOnly Property OperatorName() As String
        Get
            Return FixNull(mOperatorName)
        End Get
    End Property

    ' ReSharper disable once UnusedMember.Global
    Public ReadOnly Property OperatorPRN() As String
        Get
            Return FixNull(mOperatorPRN)
        End Get
    End Property

    ''' <summary>
    ''' Source path on the instrument, e.g. \\TSQ_1\ProteomicsData
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property SourcePath() As String
        Get
            Return FixNull(mSourcePath)
        End Get
    End Property

    Public Property TraceMode As Boolean

#End Region

#Region "Enums"
    Private Enum RawDSTypes
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
    ''' <param name="dctInstrumentsToSkip"></param>
    ''' <param name="dmsCache"></param>
    ''' <remarks></remarks>
    Public Sub New(
      mgrParams As IMgrParams,
      logger As ILogger,
      dctInstrumentsToSkip As ConcurrentDictionary(Of String, Integer),
      dmsCache As DMSInfoCache,
      udtProcSettings As clsProcessXmlTriggerFile.udtXmlProcSettingsType)

        m_mgrParams = mgrParams
        m_logger = logger

        m_FileTools = New clsFileTools
        m_InstrumentsToSkip = dctInstrumentsToSkip
        mDMSInfoCache = dmsCache
        mProcSettings = udtProcSettings
    End Sub

    Private Function FixNull(strText As String) As String
        If String.IsNullOrEmpty(strText) Then
            Return String.Empty
        Else
            Return strText
        End If
    End Function

    Public Function ValidateXMLFile(triggerFile As FileInfo) As IXMLValidateStatus.XmlValidateStatus
        Dim rslt As IXMLValidateStatus.XmlValidateStatus

        mErrorMessage = String.Empty

        Try
            If TraceMode Then clsMainProcess.ShowTraceMessage("Reading " & triggerFile.FullName)
            rslt = GetXMLParameters(triggerFile)
        Catch ex As Exception
            mErrorMessage = "Error reading the XML file " & triggerFile.Name
            Dim errMsg = "clsXMLTimeValidation.ValidateXMLFile(), " & mErrorMessage & ": " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)
            m_logger.PostEntry(errMsg, logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
        End Try

        If rslt <> IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
            Return rslt
        ElseIf m_InstrumentsToSkip.ContainsKey(mInstrumentName) Then
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SKIP_INSTRUMENT
        End If

        Try
            rslt = SetDbInstrumentParameters(mInstrumentName)
            If rslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
                rslt = PerformValidation()
            Else
                Return rslt
            End If

        Catch ex As Exception
            mErrorMessage = "Exception calling PerformValidation"
            m_logger.PostEntry("clsXMLTimeValidation.ValidateXMLFile(), Error calling PerformValidation, " & ex.Message, logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
        End Try

        Return rslt

    End Function

    ' Take the xml file and load into a dataset
    ' iterate through the dataset to retrieve the instrument name
    Private Function GetXMLParameters(triggerFile As FileInfo) As IXMLValidateStatus.XmlValidateStatus
        Dim parameterName As String
        Dim rslt As IXMLValidateStatus.XmlValidateStatus
        Dim xmlFileContents As String

        ' initialize return value
        rslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE
        xmlFileContents = LoadXmlFileContentsIntoString(triggerFile, m_logger)
        If String.IsNullOrEmpty(xmlFileContents) Then Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_TRIGGER_FILE_MISSING

        ' Load into a string reader after '&' was fixed
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
                            mInstrumentName = row("Value").ToString()
                        Case "Capture Subfolder"
                            mCaptureSubfolder = row("Value").ToString()

                            If Path.IsPathRooted(mCaptureSubfolder) Then
                                ' Instrument folder has an older version of Buzzard that incorrectly determines the capture subfolder
                                ' For safety, will blank this out, but will post a log entry to the database
                                Dim msg = "clsXMLTimeValidation.GetXMLParameters(), the CaptureSubfolder is not a relative path; " &
                                          "this indicates a bug with Buzzard; see: " & triggerFile.Name

                                m_logger.PostEntry(msg, logMsgType.logError, LOG_DATABASE)
                                mCaptureSubfolder = String.Empty
                            End If


                        Case "Dataset Name"
                            mDatasetName = row("Value").ToString()
                        Case "Run Finish UTC"
                            mRunFinishUTC = CDate(row("Value").ToString())
                        Case "Operator (PRN)"
                            mOperatorPRN = row("Value").ToString()
                    End Select
                Next row
            Next table

            If String.IsNullOrEmpty(mInstrumentName) Then
                m_logger.PostEntry("clsXMLTimeValidation.GetXMLParameters(), The instrument name was blank.", logMsgType.logError, LOG_LOCAL_ONLY)
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_BAD_XML
            End If

            If mInstrumentName.StartsWith("9T") Or mInstrumentName.StartsWith("11T") Or mInstrumentName.StartsWith("12T") Then
                rslt = InstrumentWaitDelay(triggerFile)
            End If

            If rslt <> IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE Then
                Return rslt
            Else
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE
            End If

        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.GetXMLParameters(), Error reading XML File, " & ex.Message, logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
        End Try

    End Function

    Private Function InstrumentWaitDelay(triggerFile As FileInfo) As IXMLValidateStatus.XmlValidateStatus

        Try
            Dim fileModDate As DateTime
            Dim fileModDateDelay As DateTime
            Dim dateNow As DateTime
            Dim delayValue As Integer
            delayValue = CInt(m_mgrParams.GetParam("xmlfiledelay"))

            fileModDate = triggerFile.LastWriteTimeUtc
            fileModDateDelay = fileModDate.AddMinutes(delayValue)
            dateNow = DateTime.UtcNow

            If dateNow < fileModDateDelay Then
                Dim statusMessage = "clsXMLTimeValidation.InstrumentWaitDelay(), The dataset import is being delayed for XML File: " + triggerFile.Name
                If TraceMode Then ShowTraceMessage(statusMessage)
                m_logger.PostEntry(statusMessage, logMsgType.logWarning, LOG_LOCAL_ONLY)
                Return IXMLValidateStatus.XmlValidateStatus.XML_WAIT_FOR_FILES
            End If

            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE

        Catch ex As Exception
            Dim errMsg = "clsXMLTimeValidation.InstrumentWaitDelay(), Error determining wait delay, " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)
            m_logger.PostEntry(errMsg, logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
        End Try

    End Function

    ''' <summary>
    ''' Query to get the instrument data from the database and then iterate through the dataset to retrieve the capture type and source path
    ''' </summary>
    ''' <param name="insName"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function SetDbInstrumentParameters(insName As String) As IXMLValidateStatus.XmlValidateStatus

        Try
            ' Requests additional task parameters from database and adds them to the m_taskParams string dictionary
            Dim udtInstrumentInfo = New DMSInfoCache.udtInstrumentInfoType

            If Not mDMSInfoCache.GetInstrumentInfo(insName, udtInstrumentInfo) Then
                m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Instrument " & insName & " not found in data from V_Instrument_List_Export", logMsgType.logError, LOG_LOCAL_ONLY)
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
            End If

            mCaptureType = udtInstrumentInfo.CaptureType
            mSourcePath = udtInstrumentInfo.SourcePath

            If String.IsNullOrWhiteSpace(mCaptureType) Then
                m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Instrument " & insName & " has an empty value for Capture in V_Instrument_List_Export", logMsgType.logError, LOG_DATABASE)
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
            End If

            If String.IsNullOrWhiteSpace(mSourcePath) Then
                m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Instrument " & insName & " has an empty value for SourcePath in V_Instrument_List_Export", logMsgType.logError, LOG_DATABASE)
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
            End If

            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_CONTINUE

        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.SetDbInstrumentParameters(), Error retrieving source path and capture type for instrument '" & insName & "': " & ex.Message, logMsgType.logError, LOG_LOCAL_ONLY)
            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
        End Try

    End Function

    Private Function PerformValidation() As IXMLValidateStatus.XmlValidateStatus
        Dim m_Connected = False
        Dim Pwd As String
        Dim instrumentFileOrFolderName As String = String.Empty
        Dim resType As RawDSTypes
        Dim dtFileModDate As DateTime
        Dim dtRunFinishWithTolerance As DateTime

        Dim strValue As String
        Dim intTimeValToleranceMinutes As Integer

        Dim currentTask As String = String.Empty
        Dim statusMsg As String = String.Empty

        Dim ignoreInstrumentSourceErrors = mProcSettings.IgnoreInstrumentSourceErrors

        Try

            If String.IsNullOrEmpty(mCaptureType) OrElse String.IsNullOrEmpty(mSourcePath) Then
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
            End If

            If mCaptureType = "secfso" Then
                ' Make sure mSourcePath is not of the form \\proto-2 because if that is the case, then mCaptureType should be "fso"
                Dim reProtoServer = New Text.RegularExpressions.Regex("\\\\proto-\d+\\", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)

                If reProtoServer.IsMatch(mSourcePath) Then
                    ' Auto-change mCaptureType to "fso", and log an error in the database
                    mCaptureType = "fso"
                    Dim errMsg = "Instrument " & mInstrumentName & " is configured to use 'secfso' yet its source folder is " & mSourcePath &
                        ", which appears to be a domain path; auto-changing the capture_method to 'fso' for now, " &
                        "but the configuration in the database should be updated (see table T_Instrument_Name)"
                    If TraceMode Then
                        Console.WriteLine(" - - - - - - - - ")
                        ShowTraceMessage("ERROR: " & errMsg)
                        Console.WriteLine(" - - - - - - - - ")
                    End If
                    m_logger.PostEntry(errMsg, logMsgType.logError, LOG_DATABASE)
                End If
            End If

            ' Define the source path now, before attempting to connect to Bionet
            ' This is done so that mDatasetPath will be defined so we can include it in a log message if a connection error occurs
            Dim datasetSourcePath As String
            If String.IsNullOrWhiteSpace(mCaptureSubfolder) Then
                datasetSourcePath = String.Copy(mSourcePath)
            Else
                datasetSourcePath = Path.Combine(mSourcePath, mCaptureSubfolder)
            End If

            ' Initially define this as the dataset source folder and the dataset name
            ' It will later be updated to have the actual instrument file or folder name
            mDatasetPath = Path.Combine(datasetSourcePath, mDatasetName)

            If mCaptureType = "secfso" AndAlso Not GetHostName().ToLower().StartsWith("monroe") Then
                ' Source folder is on bionet; establish a connection

                Dim m_UserName = m_mgrParams.GetParam("bionetuser")
                Dim m_Pwd = m_mgrParams.GetParam("bionetpwd")

                If Not m_UserName.Contains("\"c) Then
                    ' Prepend this computer's name to the username
                    m_UserName = GetHostName() & "\" & m_UserName
                End If

                Dim currentTaskBase = "Connecting to " & mSourcePath &
                    " using secfso, user " & m_UserName & "," &
                    " and encoded password " & m_Pwd

                currentTask = currentTaskBase & "; Decoding password"
                If TraceMode Then ShowTraceMessage(currentTask)
                Pwd = DecodePassword(m_Pwd)

                currentTask = currentTaskBase & "; Instantiating ShareConnector"
                If TraceMode Then ShowTraceMessage(currentTask)
                mShareConnector = New ShareConnector(m_UserName, Pwd)
                mShareConnector.Share = mSourcePath

                currentTask = currentTaskBase & "; Connecting using ShareConnector"
                If TraceMode Then ShowTraceMessage(currentTask)

                If mShareConnector.Connect() Then
                    m_Connected = True
                Else
                    currentTask = currentTaskBase & "; Error connecting"

                    mErrorMessage = "Error " & mShareConnector.ErrorMessage & " connecting to " & mSourcePath & " as user " & m_UserName
                    If TraceMode Then ShowTraceMessage(mErrorMessage)
                    mErrorMessage &= " using 'secfso'" + "; error code " + mShareConnector.ErrorMessage

                    m_logger.PostEntry(mErrorMessage, logMsgType.logError, LOG_LOCAL_ONLY)

                    If mShareConnector.ErrorMessage = "1326" Then
                        statusMsg = "You likely need to change the Capture_Method from secfso to fso; use the following query: "
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_logger.PostEntry(statusMsg, logMsgType.logError, LOG_LOCAL_ONLY)

                    ElseIf mShareConnector.ErrorMessage = "53" Then
                        statusMsg = "The password may need to be reset; diagnose things further using the following query: "
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_logger.PostEntry(statusMsg, logMsgType.logError, LOG_LOCAL_ONLY)

                    ElseIf mShareConnector.ErrorMessage = "1219" OrElse mShareConnector.ErrorMessage = "1203" Then
                        statusMsg = "Likely had error 'An unexpected network error occurred' while validating the Dataset specified by the XML file (ErrorMessage=" & mShareConnector.ErrorMessage & ")"
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        ' Likely had error "An unexpected network error occurred" while validating the Dataset specified by the XML file
                        ' Need to completely exit the manager
                        If mProcSettings.IgnoreInstrumentSourceErrors Then
                            ignoreInstrumentSourceErrors = True
                        Else
                            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_NETWORK_ERROR
                        End If

                    Else
                        statusMsg = "You can diagnose the problem using this query: "
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_logger.PostEntry(statusMsg, logMsgType.logError, LOG_LOCAL_ONLY)
                    End If

                    If Not ignoreInstrumentSourceErrors Then

                        If mProcSettings.IgnoreInstrumentSourceErrors Then
                            ignoreInstrumentSourceErrors = True
                        Else
                            statusMsg = "SELECT Inst.IN_name, SP.SP_path_ID, SP.SP_path, SP.SP_machine_name, SP.SP_vol_name_client, SP.SP_vol_name_server, SP.SP_function, Inst.IN_capture_method " &
                                        "FROM T_Storage_Path SP INNER JOIN T_Instrument_Name Inst ON SP.SP_instrument_name = Inst.IN_name AND SP.SP_path_ID = Inst.IN_source_path_ID " &
                                        "WHERE IN_Name = '" & mInstrumentName & "'"

                            If TraceMode Then ShowTraceMessage(statusMsg)
                            m_logger.PostEntry(statusMsg, logMsgType.logError, LOG_LOCAL_ONLY)

                            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR
                        End If
                    End If
                End If

            End If

            ' Make sure mSleepInterval isn't too large
            If mSleepInterval > 900 Then
                m_logger.PostEntry("Sleep interval of " & mSleepInterval & " seconds is too large; decreasing to 900 seconds",
                 logMsgType.logWarning, LOG_LOCAL_ONLY)
                mSleepInterval = 900
            End If

            ' Determine dataset type            
            currentTask = "Determining dataset type for " & mDatasetName & " at " & datasetSourcePath
            If TraceMode Then ShowTraceMessage(currentTask)

            ' This call updates instrumentFileOrFolderName
            resType = GetRawDSType(mSourcePath, mCaptureSubfolder, mDatasetName, ignoreInstrumentSourceErrors, instrumentFileOrFolderName)

            currentTask = "Validating operator name " & mOperatorPRN & " for " & mDatasetName & " at " & datasetSourcePath
            If TraceMode Then ShowTraceMessage(currentTask)
            If Not SetOperatorName() Then
                If m_Connected Then
                    currentTask = "Operator not found; disconnecting from " & mSourcePath
                    If TraceMode Then ShowTraceMessage(currentTask)
                    DisconnectShare(mShareConnector, m_Connected)
                End If
                Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_OPERATOR
            End If

            Select Case resType

                Case RawDSTypes.None
                    ' No raw dataset file or folder found
                    currentTask = "Dataset not found at " & datasetSourcePath
                    If TraceMode Then ShowTraceMessage(currentTask)

                    ' Disconnect from BioNet if necessary
                    If m_Connected Then
                        currentTask = "Dataset not found; disconnecting from " & mSourcePath
                        If TraceMode Then ShowTraceMessage(currentTask)
                        DisconnectShare(mShareConnector, m_Connected)
                    End If

                    Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA

                Case RawDSTypes.File
                    ' Dataset file found
                    ' Check the file size
                    currentTask = "Dataset found at " & datasetSourcePath & "; verifying file size is constant"
                    If TraceMode Then ShowTraceMessage(currentTask)

                    ' Update the dataset path to include the instrument file or folder name
                    mDatasetPath = Path.Combine(datasetSourcePath, instrumentFileOrFolderName)

                    Dim logonFailure = False

                    If ignoreInstrumentSourceErrors And Not File.Exists(mDatasetPath) Then
                        ' Assume the file is a constant size
                        statusMsg = "File not found, but assuming constant size: " & mDatasetPath
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_logger.PostEntry(statusMsg, logMsgType.logWarning, LOG_LOCAL_ONLY)

                    ElseIf Not VerifyConstantFileSize(mDatasetPath, mSleepInterval, logonFailure) Then

                        If Not logonFailure Then
                            statusMsg = "Dataset '" & mDatasetName & "' not ready (file size changed over " & mSleepInterval & " seconds)"
                            If TraceMode Then ShowTraceMessage(statusMsg)
                            m_logger.PostEntry(statusMsg, logMsgType.logWarning, LOG_LOCAL_ONLY)
                        End If

                        If m_Connected Then
                            currentTask = "Dataset size changed; disconnecting from " & mSourcePath
                            If TraceMode Then ShowTraceMessage(statusMsg)
                            DisconnectShare(mShareConnector, m_Connected)
                        End If

                        If logonFailure Then
                            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_LOGON_FAILURE
                        Else
                            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED
                        End If

                    End If

                    currentTask = "Dataset found at " & datasetSourcePath & " and is unchanged"
                    If TraceMode Then ShowTraceMessage(statusMsg)

                    If GetHostName().ToLower().StartsWith("monroe") Then
                        Console.WriteLine("Skipping date validation because host name starts with Monroe")

                    ElseIf mRunFinishUTC <> New DateTime(1960, 1, 1) Then
                        currentTask &= "; validating file date vs. Run_Finish listed in XML trigger file (" & CStr(mRunFinishUTC) & ")"
                        If TraceMode Then ShowTraceMessage(currentTask)

                        dtFileModDate = File.GetLastWriteTimeUtc(mDatasetPath)

                        strValue = m_mgrParams.GetParam("timevalidationtolerance")
                        If Not Integer.TryParse(strValue, intTimeValToleranceMinutes) Then
                            intTimeValToleranceMinutes = 800
                        End If
                        dtRunFinishWithTolerance = mRunFinishUTC.AddMinutes(intTimeValToleranceMinutes)

                        If dtFileModDate <= dtRunFinishWithTolerance Then
                            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SUCCESS
                        Else
                            statusMsg = "Time validation error for " & mDatasetName & ": file modification date (UTC): " & CStr(dtFileModDate) &
                             " vs. Run Finish UTC date: " & CStr(dtRunFinishWithTolerance) & " (includes " & intTimeValToleranceMinutes &
                             " minute tolerance)"
                            If TraceMode Then ShowTraceMessage(statusMsg)
                            m_logger.PostEntry(statusMsg, logMsgType.logError, LOG_DATABASE)
                            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED
                        End If
                    End If

                Case RawDSTypes.FolderExt, RawDSTypes.FolderNoExt
                    ' Dataset found in a folder with an extension
                    ' Verify the folder size is constant
                    currentTask = "Dataset folder found at " & datasetSourcePath & "; verifying folder size is constant for "

                    ' Update the dataset path to include the instrument file or folder name
                    mDatasetPath = Path.Combine(datasetSourcePath, instrumentFileOrFolderName)
                    currentTask &= mDatasetPath

                    If TraceMode Then ShowTraceMessage(currentTask)

                    If Not VerifyConstantFolderSize(mDatasetPath, mSleepInterval) Then
                        m_logger.PostEntry(
                         "Dataset '" & mDatasetName & "' not ready (folder size changed over " & mSleepInterval & " seconds)",
                         logMsgType.logWarning, LOG_LOCAL_ONLY)

                        If m_Connected Then
                            currentTask = "Dataset folder size changed; disconnecting from " & mSourcePath
                            DisconnectShare(mShareConnector, m_Connected)
                        End If

                        Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED
                    End If

                Case Else
                    m_logger.PostEntry("Invalid dataset type for " & mDatasetName & ": " & resType.ToString,
                     logMsgType.logError, LOG_DATABASE)
                    If m_Connected Then
                        currentTask = "Invalid dataset type; disconnecting from " & mSourcePath
                        DisconnectShare(mShareConnector, m_Connected)
                    End If

                    Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA
            End Select

            Return IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SUCCESS

        Catch ex As Exception
            m_logger.PostEntry("clsXMLTimeValidation.GetInstrumentName(), Error reading XML File, current task: " & currentTask & "; " & ex.Message, logMsgType.logError, LOG_LOCAL_ONLY)

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

    Private Function DecodePassword(encodedPwd As String) As String

        ' Decrypts password received from ini file
        ' Password was created by alternately subtracting or adding 1 to the ASCII value of each character

        Dim CharCode As Byte
        Dim TempStr As String
        Dim Indx As Integer

        TempStr = String.Empty

        Indx = 1
        Do While Indx <= Len(encodedPwd)
            CharCode = CByte(Asc(Mid(encodedPwd, Indx, 1)))
            If Indx Mod 2 = 0 Then
                CharCode = CharCode - CByte(1)
            Else
                CharCode = CharCode + CByte(1)
            End If
            TempStr = TempStr & Chr(CharCode)
            Indx = Indx + 1
        Loop

        Return TempStr

    End Function

    Private Function GetRawDSType(
      instrumentSourcePath As String,
      captureSubFolderName As String,
      currentDataset As String,
      ignoreInstrumentSourceErrors As Boolean,
      <Out()> ByRef instrumentFileOrFolderName As String) As RawDSTypes

        ' Determines if raw dataset exists as a single file, folder with same name as dataset, or 
        '	folder with dataset name + extension. Returns enum specifying what was found and instrumentFileOrFolderName
        ' containing full name of the file or folder

        ' Verify instrument transfer folder exists
        Dim diSourceFolder = New DirectoryInfo(instrumentSourcePath)
        If TraceMode Then ShowTraceMessage("Instantiated diSourceFolder with " & instrumentSourcePath)

        If Not diSourceFolder.Exists Then

            Dim msg = "Source folder not found for dataset " & currentDataset & ": " & diSourceFolder.FullName
            If TraceMode Then clsMainProcess.ShowTraceMessage(msg)

            If ignoreInstrumentSourceErrors Then
                ' Simply assume it's a Thermo .raw file
                instrumentFileOrFolderName = currentDataset & ".raw"
                Return RawDSTypes.File
            End If

            mErrorMessage = msg
            m_logger.PostEntry(mErrorMessage, logMsgType.logError, LOG_LOCAL_ONLY)
            instrumentFileOrFolderName = String.Empty
            Return RawDSTypes.None
        End If

        If Not String.IsNullOrWhiteSpace(captureSubFolderName) Then
            If captureSubFolderName.Length > 255 Then
                mErrorMessage = "Subfolder path for dataset " & currentDataset & " is too long (over 255 characters): [" & captureSubFolderName & "]"
                If TraceMode Then clsMainProcess.ShowTraceMessage(mErrorMessage)
                m_logger.PostEntry(mErrorMessage, logMsgType.logError, LOG_LOCAL_ONLY)
                instrumentFileOrFolderName = String.Empty
                Return RawDSTypes.None
            End If

            Dim diSubfolder = New DirectoryInfo(Path.Combine(diSourceFolder.FullName, captureSubFolderName))
            If Not diSubfolder.Exists Then
                mErrorMessage = "Source folder not found for dataset " & currentDataset & " in the given subfolder: [" & diSubfolder.FullName & "]"
                If TraceMode Then clsMainProcess.ShowTraceMessage(mErrorMessage)
                m_logger.PostEntry(mErrorMessage, logMsgType.logError, LOG_LOCAL_ONLY)
                instrumentFileOrFolderName = String.Empty
                Return RawDSTypes.None
            End If

            diSourceFolder = diSubfolder
        End If

        ' Check for a file with specified name
        For Each fiFile In diSourceFolder.GetFiles()
            If Path.GetFileNameWithoutExtension(fiFile.Name).ToLower() = currentDataset.ToLower() Then
                instrumentFileOrFolderName = fiFile.Name
                Return RawDSTypes.File
            End If
        Next

        ' Check for a folder with specified name
        For Each diFolder In diSourceFolder.GetDirectories()
            If Path.GetFileNameWithoutExtension(diFolder.Name).ToLower() = currentDataset.ToLower() Then
                If diFolder.Extension.Length = 0 Then
                    ' Found a directory that has no extension
                    instrumentFileOrFolderName = diFolder.Name
                    Return RawDSTypes.FolderNoExt
                Else
                    ' Directory name has an extension
                    instrumentFileOrFolderName = diFolder.Name
                    Return RawDSTypes.FolderExt
                End If
            End If
        Next

        ' If we got to here, the raw dataset wasn't found, so there was a problem
        instrumentFileOrFolderName = String.Empty
        Return RawDSTypes.None

    End Function

    Private Sub DisconnectShare(ByRef MyConn As ShareConnector, ByRef ConnState As Boolean)

        If TraceMode Then ShowTraceMessage("Disconnecting from Bionet share")

        ' Disconnects a shared drive
        MyConn.Disconnect()
        ConnState = False

    End Sub

    ''' <summary>
    ''' Determines if the size of a folder changes over specified time interval
    ''' </summary>
    ''' <param name="folderPath"></param>
    ''' <param name="sleepIntervalSeconds"></param>
    ''' <returns>True if constant, false if changed</returns>
    ''' <remarks></remarks>
    Private Function VerifyConstantFolderSize(
      folderPath As String,
      sleepIntervalSeconds As Integer) As Boolean

        ' Sleep interval should be no more than 15 minutes (900 seconds)
        If sleepIntervalSeconds > 900 Then sleepIntervalSeconds = 900
        If sleepIntervalSeconds < 1 Then sleepIntervalSeconds = 1

        ' Get the initial size of the folder
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

        ' Wait for specified sleep interval
        Thread.Sleep(sleepIntervalMsec)

        ' Get the final size of the folder and compare
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
    ''' <param name="logonFailure"></param>
    ''' <returns>True if constant, false if changed</returns>
    ''' <remarks></remarks>
    Private Function VerifyConstantFileSize(
      filePath As String,
      sleepIntervalSeconds As Integer,
      <Out()> ByRef logonFailure As Boolean) As Boolean

        ' Sleep interval should be no more than 15 minutes (900 seconds)
        If sleepIntervalSeconds > 900 Then sleepIntervalSeconds = 900
        If sleepIntervalSeconds < 1 Then sleepIntervalSeconds = 1

        logonFailure = False

        Try
            ' Get the initial size of the file
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

            ' Wait for specified sleep interval
            Thread.Sleep(sleepIntervalMsec)

            ' Get the final size of the file and compare
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
            m_logger.PostEntry(errMsg, logMsgType.logWarning, LOG_LOCAL_ONLY)

            ' Check for "Logon failure: unknown user name or bad password."

            If ex.Message.Contains("unknown user name or bad password") Then
                ' This error occasionally occurs when monitoring a .UIMF file on an IMS instrument
                ' We'll treat this as an indicator that the file size is not constant					                
                If TraceMode Then ShowTraceMessage("Error message contains 'unknown user name or bad password'; assuming this means the file size is not constant")
                logonFailure = True
            Else
                Throw
            End If
        End Try

        Return False

    End Function

    Private Function SetOperatorName() As Boolean

        Try
            If String.IsNullOrWhiteSpace(mOperatorPRN) Then
                Dim strLogMsg = "clsXMLTimeValidation.SetOperatorName: Operator field is empty (should be a network login, e.g. D3E154)"
                If TraceMode Then ShowTraceMessage(strLogMsg)
                m_logger.PostEntry(strLogMsg, logMsgType.logWarning, LOG_LOCAL_ONLY)

                mOperatorName = strLogMsg
                Return False
            End If

            Dim userCountMatched = 0
            Dim operatorInfo = mDMSInfoCache.GetOperatorName(mOperatorPRN, userCountMatched)

            ' Update the operator name, e-mail, and PRN
            mOperatorName = operatorInfo.Name
            mOperatorEmail = operatorInfo.Email
            mOperatorPRN = operatorInfo.Username

            If userCountMatched = 1 Then
                ' We matched a single user using strQueryName
                Return True
            Else
                ' We matched more 0 users, or more than one users
                ' An error should have already been logged by mDMSInfoCache
                Return False
            End If

        Catch ex As Exception
            Dim strLogMsg = "clsXMLTimeValidation.RetrieveOperatorName(), Error retrieving Operator Name, " & ex.Message
            m_logger.PostEntry(strLogMsg, logMsgType.logError, LOG_LOCAL_ONLY)
            Return False
        End Try

    End Function

End Class
