Imports System.Collections.Concurrent
Imports System.IO
Imports System.Net.Mail
Imports System.Text
Imports PRISM.Logging
Imports DataImportManager.clsGlobal

Public Class clsProcessXmlTriggerFile

#Region "Constants"
    Public Const CHECK_THE_LOG_FOR_DETAILS As String = "Check the log for details"
#End Region

#Region "Structures"
    Public Structure udtXmlProcSettingsType
        Public DebugLevel As Integer
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
    Public ReadOnly Property QueuedMail As Dictionary(Of String, List(Of clsQueuedMail))
        Get
            Return mQueuedMail
        End Get
    End Property

#End Region

#Region "Member variables"
    Private ReadOnly m_MgrSettings As clsMgrSettings
    Private ReadOnly m_InstrumentsToSkip As ConcurrentDictionary(Of String, Integer)
    Private ReadOnly mDMSInfoCache As DMSInfoCache

    Private ReadOnly m_Logger As ILogger

    Private mDataImportTask As clsDataImportTask
    Private mDatabaseErrorMsg As String

    Private m_xml_operator_Name As String = String.Empty
    Private m_xml_operator_email As String = String.Empty
    Private m_xml_dataset_path As String = String.Empty
    Private m_xml_instrument_Name As String = String.Empty

    Private ReadOnly mQueuedMail As Dictionary(Of String, List(Of clsQueuedMail))

#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="mgrSettings"></param>
    ''' <param name="instrumentsToSkip"></param>
    ''' <param name="infoCache"></param>
    ''' <param name="oLogger"></param>
    ''' <param name="udtSettings"></param>
    ''' <remarks></remarks>
    Public Sub New(
       mgrSettings As clsMgrSettings,
       instrumentsToSkip As ConcurrentDictionary(Of String, Integer),
       infoCache As DMSInfoCache,
       oLogger As ILogger,
       udtSettings As udtXmlProcSettingsType)

        m_MgrSettings = mgrSettings
        m_InstrumentsToSkip = instrumentsToSkip
        m_Logger = oLogger
        ProcSettings = udtSettings

        mDMSInfoCache = infoCache

        mQueuedMail = New Dictionary(Of String, List(Of clsQueuedMail))
    End Sub

    Private Function CreateMail(mailMsg As String, addnlRecipient As String, subjectAppend As String) As Boolean

        Dim enableEmail = CBool(m_MgrSettings.GetParam("enableemail"))
        If Not enableEmail Then
            Return False
        End If

        Try
            ' Create the mail message
            Dim mail As New MailMessage()

            ' Set the addresses
            mail.From = New MailAddress(m_MgrSettings.GetParam("from"))

            Dim mailRecipientsText = m_MgrSettings.GetParam("to")
            Dim mailRecipientsList = mailRecipientsText.Split(";"c).Distinct().ToList()

            For Each emailAddress As String In mailRecipientsList
                mail.To.Add(emailAddress)
            Next

            ' Possibly update the e-mail address for addnlRecipient
            If Not String.IsNullOrEmpty(addnlRecipient) AndAlso Not mailRecipientsList.Contains(addnlRecipient) Then
                mail.To.Add(addnlRecipient)
                mailRecipientsText &= ";" & addnlRecipient
            End If

            ' Set the Subject and Body
            If String.IsNullOrEmpty(subjectAppend) Then
                ' Data Import Manager
                mail.Subject = m_MgrSettings.GetParam("subject")
            Else
                ' Data Import Manager - Appended Info
                mail.Subject = m_MgrSettings.GetParam("subject") + subjectAppend
            End If
            mail.Body = mailMsg

            Dim statusMsg As String = "E-mailing " & mailRecipientsText & " regarding " & m_xml_dataset_path
            If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logDebug, LOG_LOCAL_ONLY)


            ' Store the message and metadata
            Dim messageToQueue = New clsQueuedMail(m_xml_operator_Name, mailRecipientsText, mail)

            If Not String.IsNullOrEmpty(mDatabaseErrorMsg) Then
                messageToQueue.DatabaseErrorMsg = mDatabaseErrorMsg
            End If

            ' Queue the message
            Dim queuedMessages As List(Of clsQueuedMail) = Nothing
            If mQueuedMail.TryGetValue(mailRecipientsText, queuedMessages) Then
                queuedMessages.Add(messageToQueue)
            Else
                queuedMessages = New List(Of clsQueuedMail)
                queuedMessages.Add(messageToQueue)
                mQueuedMail.Add(mailRecipientsText, queuedMessages)
            End If

            Return True

        Catch ex As Exception
            Dim statusMsg As String = "Error sending email message: " & ex.Message
            If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
            Return False
        End Try


    End Function

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

    Public Function ProcessFile(triggerFile As FileInfo) As Boolean

        mDatabaseErrorMsg = String.Empty

        Dim statusMsg = "Starting data import task for dataset: " & triggerFile.FullName
        If ProcSettings.TraceMode Then
            Console.WriteLine()
            Console.WriteLine("-------------------------------------------")
            ShowTraceMessage(statusMsg)
        End If
        m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

        If Not ValidateXMLFileMain(triggerFile) Then
            Return False
        End If

        If Not triggerFile.Exists Then
            statusMsg = "XML file no longer exists; cannot import: " & triggerFile.FullName
            If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
            Return False
        End If

        If ProcSettings.DebugLevel >= 2 Then
            statusMsg = "Posting Dataset XML file to database: " & triggerFile.Name
            If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
        End If

        ' Open a new database connection
        ' Doing this now due to database timeouts that were seen when using mDMSInfoCache.DBConnection

        Dim success = False

        Using dbConnection = mDMSInfoCache.GetNewDbConnection()

            ' Create the object that will import the Data record
            '
            mDataImportTask = New clsDataImportTask(m_MgrSettings, m_Logger, dbConnection)
            mDataImportTask.TraceMode = ProcSettings.TraceMode
            mDataImportTask.PreviewMode = ProcSettings.PreviewMode

            mDatabaseErrorMsg = String.Empty
            success = mDataImportTask.PostTask(triggerFile)

        End Using

        mDatabaseErrorMsg = mDataImportTask.DBErrorMessage

        If mDatabaseErrorMsg.Contains("Timeout expired.") Then
            ' Log the error and leave the file for another attempt
            statusMsg = "Encountered database timeout error for dataset: " & triggerFile.FullName
            If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_DATABASE)
            Return False
        End If

        If success Then
            MoveXmlFile(triggerFile, ProcSettings.SuccessFolder)
        Else
            If (mDataImportTask.PostTaskErrorMessage.Contains("deadlocked")) Then
                ' Log the error and leave the file for another attempt
                m_Logger.PostEntry(statusMsg & ": " & triggerFile.Name, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                Return False
            End If

            Dim messageType = ILogger.logMsgType.logError

            Dim moveLocPath = MoveXmlFile(triggerFile, ProcSettings.FailureFolder)
            statusMsg = "Error posting xml file to database: " & mDataImportTask.PostTaskErrorMessage
            If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)

            If mDataImportTask.PostTaskErrorMessage.Contains("since already in database") Then
                messageType = ILogger.logMsgType.logWarning
                m_Logger.PostEntry(statusMsg & ". See: " & moveLocPath, messageType, LOG_LOCAL_ONLY)
            Else
                m_Logger.PostEntry(statusMsg & ". View details in log at " & GetLogFileSharePath() & " for: " & moveLocPath, messageType, LOG_DATABASE)
            End If

            Dim mail_msg = New StringBuilder()
            mail_msg.AppendLine("There is a problem with the following XML file: " & moveLocPath)
            
            Dim msgTypeString As String
            If messageType = ILogger.logMsgType.logError Then
                msgTypeString = "Error"
            Else
                msgTypeString = "Warning"
            End If

            If (String.IsNullOrWhiteSpace(mDataImportTask.PostTaskErrorMessage)) Then
                mail_msg.AppendLine(msgTypeString & ": " & CHECK_THE_LOG_FOR_DETAILS)
            Else
                mail_msg.AppendLine(msgTypeString & ": " & mDataImportTask.PostTaskErrorMessage)
            End If

            ' Check whether there is a suggested solution in table T_DIM_Error_Solution for the error             
            Dim errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg)
            If Not String.IsNullOrWhiteSpace(errorSolution) Then
                ' Store the solution in the database error message variable so that it gets included in the message body
                mDatabaseErrorMsg = errorSolution
            End If

            ' Send an e-mail; subject will be "Data Import Manager - Database error." or "Data Import Manager - Database warning."
            CreateMail(mail_msg.ToString(), m_xml_operator_email, " - Database " & msgTypeString.ToLower() & ".")
            Return False
        End If

        statusMsg = "Completed Data import task for dataset: " & triggerFile.FullName
        If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
        m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

        Return success

    End Function

    Private Function MoveXmlFile(triggerFile As FileInfo, moveFolder As String) As String

        Try
            If Not triggerFile.Exists Then
                Return String.Empty
            End If

            If Not Directory.Exists(moveFolder) Then
                If ProcSettings.TraceMode Then ShowTraceMessage("Creating target folder: " + moveFolder)
                Directory.CreateDirectory(moveFolder)
            End If

            Dim targetFilePath = Path.Combine(moveFolder, triggerFile.Name)
            If ProcSettings.TraceMode Then ShowTraceMessage("Instantiating file info object for " + targetFilePath)
            Dim xmlFileNewLoc = New FileInfo(targetFilePath)

            If xmlFileNewLoc.Exists Then
                If ProcSettings.PreviewMode Then
                    ShowTraceMessage("Preview: delete target file: " + xmlFileNewLoc.FullName)
                Else
                    If ProcSettings.TraceMode Then ShowTraceMessage("Deleting target file: " + xmlFileNewLoc.FullName)
                    xmlFileNewLoc.Delete()
                End If

            End If

            If ProcSettings.PreviewMode Then
                ShowTraceMessage("Preview: move XML file from " + triggerFile.FullName + " to " + xmlFileNewLoc.DirectoryName)
            Else
                If ProcSettings.TraceMode Then ShowTraceMessage("Moving XML file from " + triggerFile.FullName + " to " + xmlFileNewLoc.DirectoryName)
                triggerFile.MoveTo(xmlFileNewLoc.FullName)
            End If

            Return xmlFileNewLoc.FullName

        Catch ex As Exception
            Dim statusMsg As String = "Exception in MoveXmlFile, " & ex.Message
            If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
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

            Dim myDataXMLValidation = New clsXMLTimeValidation(m_MgrSettings, m_Logger, m_InstrumentsToSkip, mDMSInfoCache)
            myDataXMLValidation.TraceMode = ProcSettings.TraceMode

            xmlRslt = myDataXMLValidation.ValidateXMLFile(triggerFile)

            m_xml_operator_Name = myDataXMLValidation.OperatorName()
            m_xml_operator_email = myDataXMLValidation.OperatorEMail()
            m_xml_dataset_path = myDataXMLValidation.DatasetPath()
            m_xml_instrument_Name = myDataXMLValidation.InstrumentName()

            If xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_OPERATOR Then

                moveLocPath = MoveXmlFile(triggerFile, failureFolder)

                Dim statusMsg As String = "Undefined Operator in " & moveLocPath
                If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
                m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logWarning, LOG_DATABASE)

                Dim mail_msg = New StringBuilder()
                If String.IsNullOrWhiteSpace(m_xml_operator_Name) Then
                    mail_msg.AppendLine("Operator name not listed in the XML file")
                Else
                    mail_msg.AppendLine("Operator name not defined in DMS: " & m_xml_operator_Name)
                End If

                mail_msg.AppendLine("The dataset was not added to DMS: ")
                mail_msg.AppendLine(moveLocPath)

                mDatabaseErrorMsg = "Operator payroll number/HID was blank"
                Dim errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg)
                If String.IsNullOrWhiteSpace(errorSolution) Then
                    mDatabaseErrorMsg = String.Empty
                Else
                    mDatabaseErrorMsg = errorSolution
                End If

                CreateMail(mail_msg.ToString(), m_xml_operator_email, " - Operator not defined.")
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED Then
                moveLocPath = MoveXmlFile(triggerFile, timeValFolder)

                Dim statusMsg As String = "XML Time validation error, file " & moveLocPath
                If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
                m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
                m_Logger.PostEntry("Time validation error. View details in log at " & GetLogFileSharePath() & " for: " & moveLocPath, ILogger.logMsgType.logError, LOG_DATABASE)

                Dim mail_msg = New StringBuilder()
                mail_msg.AppendLine("There was a time validation error with the following XML file: ")
                mail_msg.AppendLine(moveLocPath)
                mail_msg.AppendLine(CHECK_THE_LOG_FOR_DETAILS)
                mail_msg.AppendLine("Dataset filename and location: ")
                mail_msg.AppendLine(m_xml_dataset_path)

                CreateMail(mail_msg.ToString(), m_xml_operator_email, " - Time validation error.")
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR Then
                moveLocPath = MoveXmlFile(triggerFile, failureFolder)

                Dim statusMsg As String = "An error was encountered during the validation process, file " & moveLocPath
                If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
                m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logWarning, LOG_DATABASE)

                Dim mail_msg = New StringBuilder()
                mail_msg.AppendLine("XML error encountered during validation process for the following XML file: ")
                mail_msg.AppendLine(moveLocPath)
                mail_msg.AppendLine(CHECK_THE_LOG_FOR_DETAILS)
                mail_msg.AppendLine("Dataset filename and location: ")
                mail_msg.AppendLine(m_xml_dataset_path)

                CreateMail(mail_msg.ToString(), m_xml_operator_email, " - XML validation error.")
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

                Dim statusMsg As String = " ... skipped since m_InstrumentsToSkip contains " & m_xml_instrument_Name
                If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
                m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                UpdateInstrumentsToSkip(m_xml_instrument_Name)
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_WAIT_FOR_FILES Then
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_SIZE_CHANGED Then
                ' Size changed; Do not move the XML file
                Return False

            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA Then
                moveLocPath = MoveXmlFile(triggerFile, failureFolder)

                Dim statusMsg As String = "Dataset " & myDataXMLValidation.DatasetName & " not found at " & myDataXMLValidation.SourcePath
                If ProcSettings.TraceMode Then ShowTraceMessage(statusMsg)
                m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logWarning, LOG_DATABASE)

                Dim mail_msg = New StringBuilder()
                mail_msg.AppendLine("The dataset is not available for capture and was not added to DMS: ")
                mail_msg.AppendLine(moveLocPath)

                If String.IsNullOrEmpty(myDataXMLValidation.ErrorMessage) Then
                    mail_msg.AppendLine(CHECK_THE_LOG_FOR_DETAILS)
                Else
                    mail_msg.AppendLine(myDataXMLValidation.ErrorMessage)
                End If

                mail_msg.AppendLine("Dataset not found in following location: ")
                mail_msg.AppendLine(m_xml_dataset_path)

                mDatabaseErrorMsg = "The dataset data is not available for capture"
                Dim errorSolution = mDMSInfoCache.GetDbErrorSolution(mDatabaseErrorMsg)
                If String.IsNullOrWhiteSpace(errorSolution) Then
                    mDatabaseErrorMsg = String.Empty
                Else
                    mDatabaseErrorMsg = errorSolution
                End If

                CreateMail(mail_msg.ToString(), m_xml_operator_email, " - Dataset not found.")
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
            Dim errMsg = "Error validating Xml Data file, file " & triggerFile.FullName
            If ProcSettings.TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostError(errMsg, ex, LOG_DATABASE)
            Return False
        End Try

        Return True

    End Function
End Class
