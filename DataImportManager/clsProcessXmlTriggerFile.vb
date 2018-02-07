Imports System.Collections.Concurrent
Imports System.IO
Imports System.ServiceProcess
Imports DataImportManager.clsGlobal
Imports PRISM
Imports PRISM.Logging

Public Class clsProcessXmlTriggerFile

#Region "Constants"
    Private Const CHECK_THE_LOG_FOR_DETAILS As String = "Check the log for details"
#End Region

#Region "Structures"
    Public Structure udtXmlProcSettingsType
        Public DebugLevel As Integer
        Public IgnoreInstrumentSourceErrors As Boolean
        Public PreviewMode As Boolean
        Public TraceMode As Boolean
        Public FailureFolder As String
        Public SuccessFolder As String
    End Structure
#End Region

#Region "Properties"

    Public Property ProcSettings As udtXmlProcSettingsType

    ''' <summary>
    ''' Mail message(s) that need to be sent
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property QueuedMail As ConcurrentDictionary(Of String, ConcurrentBag(Of clsQueuedMail))
        Get
            Return mQueuedMail
        End Get
    End Property

#End Region

#Region "Member variables"
    Private ReadOnly m_MgrSettings As clsMgrSettings
    Private ReadOnly m_InstrumentsToSkip As ConcurrentDictionary(Of String, Integer)
    Private ReadOnly mDMSInfoCache As DMSInfoCache

    Private mDataImportTask As clsDataImportTask
    Private mDatabaseErrorMsg As String

    Private mSecondaryLogonServiceChecked As Boolean

    Private m_xml_operator_Name As String = String.Empty

    Private m_xml_operator_email As String = String.Empty

    ''' <summary>
    ''' Path to the dataset on the instrument
    ''' </summary>
    ''' <remarks></remarks>
    Private m_xml_dataset_path As String = String.Empty

    Private m_xml_instrument_Name As String = String.Empty

    Private ReadOnly mQueuedMail As ConcurrentDictionary(Of String, ConcurrentBag(Of clsQueuedMail))

#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="mgrSettings"></param>
    ''' <param name="instrumentsToSkip"></param>
    ''' <param name="infoCache"></param>
    ''' <param name="udtSettings"></param>
    ''' <remarks></remarks>
    Public Sub New(
       mgrSettings As clsMgrSettings,
       instrumentsToSkip As ConcurrentDictionary(Of String, Integer),
       infoCache As DMSInfoCache,
       udtSettings As udtXmlProcSettingsType)

        m_MgrSettings = mgrSettings
        m_InstrumentsToSkip = instrumentsToSkip
        ProcSettings = udtSettings

        mDMSInfoCache = infoCache

        mQueuedMail = New ConcurrentDictionary(Of String, ConcurrentBag(Of clsQueuedMail))
    End Sub

    Private Sub CacheMail(validationErrors As List(Of clsValidationError), addnlRecipient As String, subjectAppend As String)

        Dim enableEmail = CBool(m_MgrSettings.GetParam("enableemail"))
        If Not enableEmail Then
            Return
        End If

        Try

            Dim mailRecipients = m_MgrSettings.GetParam("to")
            Dim mailRecipientsList = mailRecipients.Split(";"c).Distinct().ToList()

            ' Possibly update the e-mail address for addnlRecipient
            If Not String.IsNullOrEmpty(addnlRecipient) AndAlso Not mailRecipientsList.Contains(addnlRecipient) Then
                mailRecipients &= ";" & addnlRecipient
            End If

            ' Define the Subject
            Dim mailSubject As String
            If String.IsNullOrEmpty(subjectAppend) Then
                ' Data Import Manager
                mailSubject = m_MgrSettings.GetParam("subject")
            Else
                ' Data Import Manager - Appended Info
                mailSubject = m_MgrSettings.GetParam("subject") + subjectAppend
            End If

            ' Store the message and metadata
            Dim messageToQueue = New clsQueuedMail(m_xml_operator_Name, mailRecipients, mailSubject, validationErrors)

            If Not String.IsNullOrEmpty(mDatabaseErrorMsg) Then
                messageToQueue.DatabaseErrorMsg = mDatabaseErrorMsg
            End If

            messageToQueue.InstrumentDatasetPath = m_xml_dataset_path

            ' Queue the message
            Dim existingQueuedMessages As ConcurrentBag(Of clsQueuedMail) = Nothing
            If mQueuedMail.TryGetValue(mailRecipients, existingQueuedMessages) Then
                existingQueuedMessages.Add(messageToQueue)
            Else
                Dim newQueuedMessages = New ConcurrentBag(Of clsQueuedMail)
                newQueuedMessages.Add(messageToQueue)

                If Not mQueuedMail.TryAdd(mailRecipients, newQueuedMessages) Then
                    If mQueuedMail.TryGetValue(mailRecipients, existingQueuedMessages) Then
                        existingQueuedMessages.Add(messageToQueue)
                    End If
                End If

            End If

            Return

        Catch ex As Exception
            LogTools.LogError("Error sending email message", ex)
            Return
        End Try


    End Sub

    ''' <summary>
    ''' Returns a string with the path to the log file, assuming the file can be accessed with \\ComputerName\DMS_Programs\ProgramFolder\Logs\LogFileName.txt
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function GetLogFileSharePath() As String
        Dim logFileName = m_MgrSettings.GetParam("logfilename")
        Return GetLogFileSharePath(logFileName)
    End Function

    ''' <summary>
    ''' Returns a string with the path to the log file, assuming the file can be accessed with \\ComputerName\DMS_Programs\ProgramFolder\Logs\LogFileName.txt
    ''' </summary>
    ''' <param name="logFileName">Name of the current log file</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function GetLogFileSharePath(logFileName As String) As String

        Dim strLogFilePath As String

        Dim fiExe = New FileInfo(GetExePath())

        If String.IsNullOrEmpty(logFileName) Then
            strLogFilePath = Path.Combine(fiExe.Directory.Name, "DataImportManager")
        Else
            strLogFilePath = Path.Combine(fiExe.Directory.Name, logFileName)
        End If

        ' strLogFilePath should look like this:
        '	DataImportManager\Logs\DataImportManager

        ' Prepend the computer name and share name, giving a string similar to:
        ' \\proto-3\DMS_Programs\DataImportManager\Logs\DataImportManager

        strLogFilePath = "\\" & GetHostName() & "\DMS_Programs\" & strLogFilePath

        ' Append the date stamp to the log
        strLogFilePath &= "_" & DateTime.Now.ToString("MM-dd-yyyy") & ".txt"

        Return strLogFilePath

    End Function

    ''' <summary>
    ''' Validate the XML trigger file, then send it to the database using mDataImportTask.PostTask
    ''' </summary>
    ''' <param name="triggerFile"></param>
    ''' <returns>True if success, false if an error</returns>
    Public Function ProcessFile(triggerFile As FileInfo) As Boolean

        mDatabaseErrorMsg = String.Empty

        Dim statusMsg = "Starting data import task for dataset: " & triggerFile.FullName
        If ProcSettings.TraceMode Then
            Console.WriteLine()
            Console.WriteLine("-------------------------------------------")
        End If
        LogTools.LogMessage(statusMsg)

        If Not ValidateXMLFileMain(triggerFile) Then

            If mSecondaryLogonServiceChecked Then
                Return False
            End If

            mSecondaryLogonServiceChecked = True

            ' Check the status of the Secondary Logon service
            Dim sc = New ServiceController("seclogon")
            If sc.Status = ServiceControllerStatus.Running Then
                Return False
            End If

            clsMainProcess.LogErrorToDatabase("The Secondary Logon service is not running; this is required to access files on Bionet")

            Try
                ' Try to start it
                LogTools.LogMessage("Attempting to start the Secondary Logon service")

                sc.Start()

                clsProgRunner.SleepMilliseconds(3000)

                statusMsg = "Successfully started the Secondary Logon service (normally should be running, but found to be stopped)"
                LogTools.LogWarning(statusMsg, True)

                ' Now that the service is running, try the validation one more time
                If Not ValidateXMLFileMain(triggerFile) Then
                    Return False
                End If

            Catch ex As Exception
                LogTools.LogWarning("Unable to start the Secondary Logon service: " & ex.Message)
                Return False
            End Try

        End If

        If Not triggerFile.Exists Then
            LogTools.LogWarning("XML file no longer exists; cannot import: " & triggerFile.FullName)
            Return False
        End If

        If ProcSettings.DebugLevel >= 2 Then
            LogTools.LogMessage("Posting Dataset XML file to database: " & triggerFile.Name)
        End If

        ' Open a new database connection
        ' Doing this now due to database timeouts that were seen when using mDMSInfoCache.DBConnection

        Dim dbConnection = mDMSInfoCache.GetNewDbConnection()

        ' Create the object that will import the Data record
        '
        mDataImportTask = New clsDataImportTask(m_MgrSettings, dbConnection)
        mDataImportTask.TraceMode = ProcSettings.TraceMode
        mDataImportTask.PreviewMode = ProcSettings.PreviewMode

        mDatabaseErrorMsg = String.Empty
        Dim success = mDataImportTask.PostTask(triggerFile)

        mDatabaseErrorMsg = mDataImportTask.DBErrorMessage

        If mDatabaseErrorMsg.ToLower().Contains("timeout expired.") Then
            ' Log the error and leave the file for another attempt
            clsMainProcess.LogErrorToDatabase("Encountered database timeout error for dataset: " & triggerFile.FullName)
            Return False
        End If

        If success Then
            MoveXmlFile(triggerFile, ProcSettings.SuccessFolder)
            LogTools.LogMessage("Completed Data import task for dataset: " & triggerFile.FullName)
            Return True

        End If

        ' Look for:
        ' Transaction (Process ID 67) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction
        If (mDataImportTask.PostTaskErrorMessage.ToLower().Contains("deadlocked")) Then
            ' Log the error and leave the file for another attempt
            statusMsg = "Deadlock encountered"
            LogTools.LogError(statusMsg & ": " & triggerFile.Name)
            Return False
        End If

        ' Look for:
        ' The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction
        If (mDataImportTask.PostTaskErrorMessage.ToLower().Contains("current transaction cannot be committed")) Then
            ' Log the error and leave the file for another attempt
            statusMsg = "Transaction commit error"
            LogTools.LogError(statusMsg & ": " & triggerFile.Name)
            Return False
        End If

        Dim messageType As BaseLogger.LogLevels

        Dim moveLocPath = MoveXmlFile(triggerFile, ProcSettings.FailureFolder)
        statusMsg = "Error posting xml file to database: " & mDataImportTask.PostTaskErrorMessage

        If mDataImportTask.PostTaskErrorMessage.ToLower().Contains("since already in database") Then
            messageType = BaseLogger.LogLevels.WARN
            LogTools.LogWarning(statusMsg & ". See: " & moveLocPath)
        Else
            messageType = BaseLogger.LogLevels.ERROR
            clsMainProcess.LogErrorToDatabase(statusMsg & ". View details in log at " & GetLogFileSharePath() & " for: " & moveLocPath)
        End If

        Dim validationErrors = New List(Of clsValidationError)
        Dim newError = New clsValidationError("XML trigger file problem", moveLocPath)

        Dim msgTypeString As String
        If messageType = BaseLogger.LogLevels.ERROR Then
            msgTypeString = "Error"
        Else
            msgTypeString = "Warning"
        End If

        If (String.IsNullOrWhiteSpace(mDataImportTask.PostTaskErrorMessage)) Then
            newError.AdditionalInfo = msgTypeString & ": " & CHECK_THE_LOG_FOR_DETAILS
        Else
            newError.AdditionalInfo = msgTypeString & ": " & mDataImportTask.PostTaskErrorMessage
        End If

        validationErrors.Add(newError)

        ' Check whether there is a suggested solution in table T_DIM_Error_Solution for the error
        Dim errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg)
        If Not String.IsNullOrWhiteSpace(errorSolution) Then
            ' Store the solution in the database error message variable so that it gets included in the message body
            mDatabaseErrorMsg = errorSolution
        End If

        ' Send an e-mail; subject will be "Data Import Manager - Database error." or "Data Import Manager - Database warning."
        CacheMail(validationErrors, m_xml_operator_email, " - Database " & msgTypeString.ToLower() & ".")
        Return False

    End Function

    ''' <summary>
    ''' Move a trigger file to the target folder
    ''' </summary>
    ''' <param name="triggerFile"></param>
    ''' <param name="moveFolder"></param>
    ''' <returns>New path of the trigger file</returns>
    ''' <remarks></remarks>
    Private Function MoveXmlFile(triggerFile As FileInfo, moveFolder As String) As String

        Try
            If Not triggerFile.Exists Then
                Return String.Empty
            End If

            If Not Directory.Exists(moveFolder) Then
                If ProcSettings.TraceMode Then ShowTraceMessage("Creating target folder: " & moveFolder)
                Directory.CreateDirectory(moveFolder)
            End If

            Dim targetFilePath = Path.Combine(moveFolder, triggerFile.Name)
            If ProcSettings.TraceMode Then ShowTraceMessage("Instantiating file info object for " & targetFilePath)
            Dim xmlFileNewLoc = New FileInfo(targetFilePath)

            If xmlFileNewLoc.Exists Then
                If ProcSettings.PreviewMode Then
                    ShowTraceMessage("Preview: delete target file: " & xmlFileNewLoc.FullName)
                Else
                    If ProcSettings.TraceMode Then ShowTraceMessage("Deleting target file: " & xmlFileNewLoc.FullName)
                    xmlFileNewLoc.Delete()
                End If

            End If

            Dim movePaths = "XML file " & ControlChars.NewLine & "  from " & triggerFile.FullName & ControlChars.NewLine & "  to   " & xmlFileNewLoc.DirectoryName

            If ProcSettings.PreviewMode Then
                ShowTraceMessage("Preview: move " & movePaths)
            Else
                If ProcSettings.TraceMode Then ShowTraceMessage("Moving " & movePaths)
                triggerFile.MoveTo(xmlFileNewLoc.FullName)
            End If

            Return xmlFileNewLoc.FullName

        Catch ex As Exception
            LogTools.LogError("Exception in MoveXmlFile", ex)
            Return String.Empty
        End Try

    End Function

    ''' <summary>
    ''' Adds or updates strInstrumentName in m_InstrumentsToSkip
    ''' </summary>
    ''' <param name="strInstrumentName"></param>
    ''' <remarks></remarks>
    Private Sub UpdateInstrumentsToSkip(strInstrumentName As String)

        Dim intDatasetsSkipped = 0
        If m_InstrumentsToSkip.TryGetValue(strInstrumentName, intDatasetsSkipped) Then
            m_InstrumentsToSkip(strInstrumentName) = intDatasetsSkipped + 1
        Else
            If Not m_InstrumentsToSkip.TryAdd(strInstrumentName, 1) Then
                If m_InstrumentsToSkip.TryGetValue(strInstrumentName, intDatasetsSkipped) Then
                    m_InstrumentsToSkip(strInstrumentName) = intDatasetsSkipped + 1
                End If
            End If
        End If
    End Sub

    ''' <summary>
    ''' Process the specified XML file
    ''' </summary>
    ''' <param name="triggerFile">XML file to process</param>
    ''' <returns>True if XML file is valid and dataset is ready for import; otherwise false</returns>
    ''' <remarks></remarks>
    Private Function ValidateXMLFileMain(triggerFile As FileInfo) As Boolean

        Try
            Dim xmlRslt As IXMLValidateStatus.XmlValidateStatus
            Dim timeValFolder As String = m_MgrSettings.GetParam("timevalidationfolder")
            Dim moveLocPath As String
            Dim failureFolder As String = m_MgrSettings.GetParam("failurefolder")

            Dim myDataXMLValidation = New clsXMLTimeValidation(m_MgrSettings, m_InstrumentsToSkip, mDMSInfoCache, ProcSettings)
            myDataXMLValidation.TraceMode = ProcSettings.TraceMode

            xmlRslt = myDataXMLValidation.ValidateXMLFile(triggerFile)

            m_xml_operator_Name = myDataXMLValidation.OperatorName()
            m_xml_operator_email = myDataXMLValidation.OperatorEMail()
            m_xml_dataset_path = myDataXMLValidation.DatasetPath()
            m_xml_instrument_Name = myDataXMLValidation.InstrumentName()

            If xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_OPERATOR Then

                moveLocPath = MoveXmlFile(triggerFile, failureFolder)

                LogTools.LogWarning("Undefined Operator in " & moveLocPath, True)

                Dim validationErrors = New List(Of clsValidationError)

                If String.IsNullOrWhiteSpace(m_xml_operator_Name) Then
                    validationErrors.Add(New clsValidationError("Operator name not listed in the XML file", String.Empty))
                Else
                    validationErrors.Add(New clsValidationError("Operator name not defined in DMS", m_xml_operator_Name))
                End If

                validationErrors.Add(New clsValidationError("Dataset trigger file path", moveLocPath))

                mDatabaseErrorMsg = "Operator payroll number/HID was blank"
                Dim errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg)
                If String.IsNullOrWhiteSpace(errorSolution) Then
                    mDatabaseErrorMsg = String.Empty
                Else
                    mDatabaseErrorMsg = errorSolution
                End If

                CacheMail(validationErrors, m_xml_operator_email, " - Operator not defined.")
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED Then
                moveLocPath = MoveXmlFile(triggerFile, timeValFolder)

                LogTools.LogWarning("XML Time validation error, file " & moveLocPath)
                clsMainProcess.LogErrorToDatabase("Time validation error. View details in log at " & GetLogFileSharePath() & " for: " & moveLocPath)

                Dim validationErrors = New List(Of clsValidationError)
                validationErrors.Add(New clsValidationError("Time validation error", moveLocPath))

                CacheMail(validationErrors, m_xml_operator_email, " - Time validation error.")
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR Then
                moveLocPath = MoveXmlFile(triggerFile, failureFolder)

                LogTools.LogWarning("An error was encountered during the validation process, file " & moveLocPath, True)

                Dim validationErrors = New List(Of clsValidationError)
                validationErrors.Add(New clsValidationError("XML error encountered during validation process", moveLocPath))

                CacheMail(validationErrors, m_xml_operator_email, " - XML validation error.")
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

                LogTools.LogMessage(" ... skipped since m_InstrumentsToSkip contains " & m_xml_instrument_Name)
                UpdateInstrumentsToSkip(m_xml_instrument_Name)
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_WAIT_FOR_FILES Then
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED Then
                ' Size changed; Do not move the XML file
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA Then
                moveLocPath = MoveXmlFile(triggerFile, failureFolder)

                LogTools.LogWarning("Dataset " & myDataXMLValidation.DatasetName & " not found at " & myDataXMLValidation.SourcePath, True)

                Dim validationErrors = New List(Of clsValidationError)

                Dim newError = New clsValidationError("Dataset not found on the instrument", moveLocPath)
                If String.IsNullOrEmpty(myDataXMLValidation.ErrorMessage) Then
                    newError.AdditionalInfo = String.Empty
                Else
                    newError.AdditionalInfo = myDataXMLValidation.ErrorMessage
                End If
                validationErrors.Add(newError)

                mDatabaseErrorMsg = "The dataset data is not available for capture"
                Dim errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg)
                If String.IsNullOrWhiteSpace(errorSolution) Then
                    mDatabaseErrorMsg = String.Empty
                Else
                    mDatabaseErrorMsg = errorSolution
                End If

                CacheMail(validationErrors, m_xml_operator_email, " - Dataset not found.")

                Return False
            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_TRIGGER_FILE_MISSING Then
                ' The file is now missing; silently move on
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
            clsMainProcess.LogErrorToDatabase("Error validating Xml Data file, file " & triggerFile.FullName, ex)
            Return False
        End Try

        Return True

    End Function
End Class

