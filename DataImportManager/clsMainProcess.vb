Imports System.Net.Mail
Imports System.IO
Imports DataImportManager
Imports DataImportManager.clsGlobal
Imports DataImportManager.MgrSettings
Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports System.Collections.Specialized
Imports System.Windows.Forms

Public Class clsMainProcess
    '**
    ' This is the main class that does the following:

#Region "Constants"
    Private Const EMERG_LOG_FILE As String = "C:\DataImportMan_log.txt"
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
    Private m_XmlFilesToLoad As New StringDictionary
    Public m_db_Err_Msg As String
    Private m_xml_operator_Name As String
    Private m_xml_operator_email As String
    Dim m_ImportStatusCount As Integer = 0
    Private myDataXMLValidation As clsXMLTimeValidation
#End Region

    Public Sub New()

    End Sub

    Private Function InitMgr() As Boolean
        Dim LogFile As String
        Dim ConnectStr As String
        Dim ModName As String

        'Get the manager settings
        Try
            m_MgrSettings = New clsMgrSettings(EMERG_LOG_FILE)
        Catch ex As Exception
            Throw New Exception("clsMainProcess.New(), " & ex.Message)
            'Failures are logged by clsMgrSettings to local emergency log file
            Return False
        End Try

        Dim FInfo As FileInfo = New FileInfo(Application.ExecutablePath)
        Try
            'Load initial settings
            m_MgrActive = CBool(m_MgrSettings.GetParam("mgractive"))
            m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

            ' create the object that will manage the logging
            LogFile = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("logfilename"))
            ConnectStr = m_MgrSettings.GetParam("connectionstring")
            ModName = m_MgrSettings.GetParam("modulename")
            m_Logger = New clsQueLogger(New clsDBLogger(ModName, ConnectStr, LogFile))

            'Write the initial log and status entries
            m_Logger.PostEntry("===== Started Data Import Manager V" & Application.ProductVersion & " =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

        Catch Err As System.Exception
            Throw New Exception("clsMainProcess.New(), " & Err.Message)
            Exit Function
        End Try

        'Setup the logger
        Dim LogFileName As String = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("logfilename"))
        Dim DbLogger As New clsDBLogger
        DbLogger.LogFilePath = LogFileName
        DbLogger.ConnectionString = m_MgrSettings.GetParam("connectionstring")
        DbLogger.ModuleName = m_MgrSettings.GetParam("modulename")
        m_Logger = New clsQueLogger(DbLogger)
        DbLogger = Nothing

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

        'Everything worked
        Return True

    End Function

    Shared Sub Main()

        Dim ErrMsg As String

        Try
            clsGlobal.AppFilePath = Application.ExecutablePath
            If IsNothing(m_StartupClass) Then
                m_StartupClass = New clsMainProcess
                If Not m_StartupClass.InitMgr() Then Exit Sub
            End If
            m_StartupClass.DoImport()
        Catch Err As System.Exception
            'Report any exceptions not handled at a lower level to the system application log
            ErrMsg = "Critical exception starting application: " & Err.Message
            Dim Ev As New EventLog("Application", ".", "DMSDataImportManager")
            Trace.Listeners.Add(New EventLogTraceListener("DMSDataImportManager"))
            Trace.WriteLine(ErrMsg)
            Ev.Close()
            Exit Sub
        End Try

    End Sub

    Public Sub DoImport()

        Try

            'Verify an error hasn't left the the system in an odd state
            If DetectStatusFlagFile() Then
                m_Logger.PostEntry("Flag file exists - unable to perform any further data import tasks", _
                ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                m_Logger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                Exit Sub
            End If

            'Check to see if machine settings have changed
            If m_ConfigChanged Then
                m_ConfigChanged = False
                If Not m_MgrSettings.LoadSettings(True) Then
                    If m_MgrSettings.ErrMsg <> "" Then
                        'Manager has been deactivated, so report this
                        m_Logger.PostEntry(m_MgrSettings.ErrMsg, ILogger.logMsgType.logWarning, True)
                    Else
                        'Unknown problem reading config file
                        m_Logger.PostEntry("Error re-reading config file", ILogger.logMsgType.logError, True)
                    End If
                    m_Logger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, True)
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
                m_Logger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                Exit Sub
            End If

            'Check to see if there are any data import files ready
            DoDataImportTask()
            m_Logger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
        Catch Err As System.Exception
            m_Logger.PostEntry("clsMainProcess.DoImport(), " & Err.Message, ILogger.logMsgType.logError, True)
            m_Logger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
            Exit Sub
        End Try

    End Sub

    Private Sub DoDataImportTask()

        Dim result As ITaskParams.CloseOutType
        Dim rslt As Boolean
        Dim ModName As String = m_MgrSettings.GetParam("modulename")
        Dim XferDir As String = m_MgrSettings.GetParam("xferdir")
        Dim runStatus As ITaskParams.CloseOutType = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS
        Dim DelBadXmlFilesDays As Integer = CInt(m_MgrSettings.GetParam("deletebadxmlfiles"))
        Dim DelGoodXmlFilesDays As Integer = CInt(m_MgrSettings.GetParam("deletegoodxmlfiles"))
        Dim successFolder As String = m_MgrSettings.GetParam("successfolder")
        Dim failureFolder As String = m_MgrSettings.GetParam("failurefolder")
        Dim moveLocPath As String = ""
        Dim mail_msg As String = ""

        Try

            result = ScanXferDirectory()

            If result = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS And m_XmlFilesToLoad.Count > 0 Then

                CreateStatusFlagFile()    'Set status file for control of future runs


                ' create the object that will import the Data record
                '
                myDataImportTask = New clsDataImportTask(m_MgrSettings, m_Logger)
                '
                Application.DoEvents()

                'Add a delay
                Dim importDelay As String = m_MgrSettings.GetParam("importdelay")
                System.Threading.Thread.Sleep(CInt(importDelay) * 1000)

                Dim myEnumerator As IEnumerator = m_XmlFilesToLoad.GetEnumerator()
                Dim de As DictionaryEntry

                For Each de In m_XmlFilesToLoad
                    'Validate the xml file
                    If ValidateXMLFile(CStr(de.Key)) Then
                        m_Logger.PostEntry(ModName & ": Started Data import task for dataset: " & CStr(de.Key), ILogger.logMsgType.logNormal, LOG_DATABASE)
                        m_db_Err_Msg = ""
                        rslt = myDataImportTask.PostTask(CStr(de.Key))
                        m_db_Err_Msg = myDataImportTask.mp_db_err_msg
                        If m_db_Err_Msg.Contains("Timeout expired.") Then
                            'post the error and leave the file for another attempt
                            m_Logger.PostEntry(ModName & ": Encountered database timeout error for dataset: " & CStr(de.Key), ILogger.logMsgType.logNormal, LOG_DATABASE)
                        Else
                            If rslt Then
                                moveLocPath = MoveXmlFile(CStr(de.Key), successFolder)
                            Else
                                'myDataImportTask.GetReturnValue()
                                moveLocPath = MoveXmlFile(CStr(de.Key), failureFolder)
                                m_Logger.PostEntry("Error posting xml file to database. View details in log for: " & moveLocPath, ILogger.logMsgType.logError, LOG_DATABASE)
                                mail_msg = "There is a problem with the following XML file: " & moveLocPath & ".  Check the log for details."
                                mail_msg = mail_msg + Chr(13) & Chr(10) & "Operator: " & m_xml_operator_Name
                                rslt = myDataImportTask.GetDbErrorSolution(m_db_Err_Msg)
                                CreateMail(mail_msg, m_xml_operator_email, " - Database error.")
                            End If
                            m_Logger.PostEntry(ModName & ": Completed Data import task for dataset: " & CStr(de.Key), ILogger.logMsgType.logNormal, LOG_DATABASE)
                        End If
                    End If
                Next de
                m_XmlFilesToLoad.Clear()

            Else
                m_Logger.PostEntry(ModName & ": No Data Files to import.", ILogger.logMsgType.logNormal, LOG_DATABASE)
                m_Logger.PostEntry(ModName & ": No Data Files to import.", ILogger.logMsgType.logHealth, LOG_DATABASE)
                Exit Sub
            End If

            'Remove successful XML files older than x days
            DeleteXmlFiles(successFolder, DelGoodXmlFilesDays)

            'Remove failed XML files older than x days
            DeleteXmlFiles(failureFolder, DelBadXmlFilesDays)

            ' If we got to here, then closeout the task as a success
            '
            DeleteStatusFlagFile(m_Logger)
            FailCount = 0
            If runStatus = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS Then
                m_Logger.PostEntry(ModName & ": Completed task ", ILogger.logMsgType.logNormal, LOG_DATABASE)
            End If

        Catch Err As System.Exception
            FailCount += 1
            m_Logger.PostEntry("clsMainProcess.DoDataImportTask(), " & Err.Message, ILogger.logMsgType.logError, True)
            Exit Sub
        End Try

    End Sub

    Public Function ScanXferDirectory() As ITaskParams.CloseOutType

        'Copies the results to the transfer directory
        Dim ServerXferDir As String = m_MgrSettings.GetParam("xferdir")
        Dim filedate As DateTime

        'Verify transfer directory exists
        If Not Directory.Exists(ServerXferDir) Then
            'There's a serious problem is the xfer directory can't be found!!!
            m_Logger.PostEntry("Xml transfer folder not found. ", ILogger.logMsgType.logError, LOG_DATABASE)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Clear the string dictionary
        m_XmlFilesToLoad.Clear()

        'Load all the Xml File names and dates in the transfer directory into a string dictionary
        Try
            Dim XmlFilesToImport() As String = Directory.GetFiles(Path.Combine(ServerXferDir, ServerXferDir))
            For Each XmlFile As String In XmlFilesToImport
                filedate = File.GetLastWriteTime(Path.Combine(ServerXferDir, Path.GetFileName(XmlFile)))
                m_XmlFilesToLoad.Add(XmlFile, CStr(filedate))
            Next
        Catch err As System.Exception
            m_Logger.PostError("Error loading Xml Data files", err, LOG_DATABASE)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End Try
        m_XmlFilesToLoad.Values.GetEnumerator()
        'Everything must be OK if we got to here
        Return ITaskParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    Private Function MoveXmlFile(ByVal xmlFile As String, ByVal moveFolder As String) As String

        Dim Fi As FileInfo
        'Dim xmlFilePath As String = ""
        Dim xmlFileName As String = ""
        Dim xmlFileNewLoc As String = ""
        Try
            If File.Exists(xmlFile) Then
                Fi = New FileInfo(xmlFile)
                xmlFileName = Fi.Name
                If Not Directory.Exists(moveFolder) Then
                    Directory.CreateDirectory(moveFolder)
                End If

                xmlFileNewLoc = Path.Combine(moveFolder, xmlFileName)
                If File.Exists(xmlFileNewLoc) Then
                    File.Delete(xmlFileNewLoc)
                End If
                File.Move(xmlFile, xmlFileNewLoc)

            End If
        Catch Err As System.Exception
            m_Logger.PostEntry("MoveXmlFile, " & Err.Message, ILogger.logMsgType.logError, True)
        End Try
        Return xmlFileNewLoc
    End Function
    Private Function GetDirectory(ByVal xmlFile As String) As String

        Dim Fi As FileInfo
        Dim xmlFilePath As String = ""
        Try
            If File.Exists(xmlFile) Then
                Fi = New FileInfo(xmlFile)
                xmlFilePath = Fi.DirectoryName
            End If
        Catch Err As System.Exception
            m_Logger.PostEntry("GetDirectory, " & Err.Message, ILogger.logMsgType.logError, True)
        End Try
        Return xmlFilePath

    End Function

    Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles m_FileWatcher.Changed
        m_ConfigChanged = True
        If m_DebugLevel > 3 Then
            m_Logger.PostEntry("Config file changed", ILogger.logMsgType.logDebug, True)
        End If
        m_FileWatcher.EnableRaisingEvents = False  'Turn off change detection until current change has been acted upon
    End Sub

    Function CreateMail(ByVal mailMsg As String, ByVal addtnlRecipient As String, ByVal overrideSubject As String) As Boolean
        Dim ErrMsg As String
        Dim addMsg As String
        Dim enableEmail As Boolean
        Dim emailAddress As String

        enableEmail = CBool(m_MgrSettings.GetParam("enableemail"))
        If enableEmail Then
            Try
                addMsg = Chr(13) & Chr(10) & Chr(13) & Chr(10) & "(NOTE: This message was sent from an account that is not monitored. If you have any questions, please reply to the list of recipients directly.)"
                'create the mail message
                Dim mail As New System.Net.Mail.MailMessage()
                'set the addresses
                mail.From = New MailAddress(m_MgrSettings.GetParam("from"))
                For Each emailAddress In Split(m_MgrSettings.GetParam("to"), ";")
                    mail.To.Add(emailAddress)
                Next
                If addtnlRecipient <> "" Then
                    mail.To.Add(addtnlRecipient)
                End If

                'mail.To.Add(m_MgrSettings.GetParam("to"))
                'set the content
                If overrideSubject = "" Then
                    mail.Subject = m_MgrSettings.GetParam("subject")
                Else
                    mail.Subject = m_MgrSettings.GetParam("subject") + overrideSubject
                End If
                mail.Body = mailMsg & Chr(13) & Chr(10) & Chr(13) & Chr(10) & m_db_Err_Msg & addMsg
                'send the message
                Dim smtp As New SmtpClient(m_MgrSettings.GetParam("smtpserver"))
                'to authenticate we set the username and password properites on the SmtpClient
                smtp.Send(mail)
            Catch Ex As Exception
                ErrMsg = "Exception: " & Ex.Message & vbCrLf & vbCrLf & "Sending mail message."
                m_Logger.PostEntry("Error creating email message", ILogger.logMsgType.logError, True)
            End Try
        End If

    End Function

    Private Function DeleteXmlFiles(ByVal FileDirectory As String, ByVal NoDays As Integer) As ITaskParams.CloseOutType

        Dim filedate As DateTime
        Dim daysDiff As Integer
        Dim workDirectory As String

        workDirectory = FileDirectory
        'Verify directory exists
        If Not Directory.Exists(workDirectory) Then
            'There's a serious problem if the success/failure directory can't be found!!!
            m_Logger.PostEntry("Xml success/failure folder not found.", ILogger.logMsgType.logError, LOG_DATABASE)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Load all the Xml File names and dates in the transfer directory into a string dictionary
        Try
            Dim XmlFilesToDelete() As String = Directory.GetFiles(Path.Combine(workDirectory, workDirectory))
            For Each XmlFile As String In XmlFilesToDelete
                filedate = File.GetLastWriteTime(XmlFile)
                daysDiff = Now.Subtract(filedate).Days
                If Now.Subtract(filedate).Days > NoDays Then
                    File.Delete(XmlFile)
                End If
            Next
        Catch err As System.Exception
            m_Logger.PostError("Error deleting Xml Data files", err, LOG_DATABASE)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Everything must be OK if we got to here
        Return ITaskParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Sub DisableManagerLocally()

        If Not m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False") Then
            m_Logger.PostEntry("Error while disabling manager: " & m_MgrSettings.ErrMsg, ILogger.logMsgType.logError, True)
        End If

    End Sub

    Private Function ValidateXMLFile(ByVal xmlFilename As String) As Boolean

        Try
            Dim ModName As String = m_MgrSettings.GetParam("modulename")
            Dim xmlRslt As IXMLValidateStatus.XmlValidateStatus
            Dim timeValFolder As String = m_MgrSettings.GetParam("timevalidationfolder")
            Dim moveLocPath As String = ""
            Dim mail_msg As String = ""
            Dim failureFolder As String = m_MgrSettings.GetParam("failurefolder")
            Dim rslt As Boolean
            myDataImportTask = New clsDataImportTask(m_MgrSettings, m_Logger)

            myDataXMLValidation = New clsXMLTimeValidation(m_MgrSettings, m_Logger)

            xmlRslt = myDataXMLValidation.ValidateXMLFile(xmlFilename)
            m_xml_operator_Name = myDataXMLValidation.m_operator_Name
            m_xml_operator_email = myDataXMLValidation.m_operator_Email
            If xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_FAILED Then
                m_Logger.PostEntry(ModName & ": XML Time validation error.", ILogger.logMsgType.logNormal, LOG_DATABASE)
                m_Logger.PostEntry(ModName & ": XML Time validation error.", ILogger.logMsgType.logHealth, LOG_DATABASE)
                moveLocPath = MoveXmlFile(xmlFilename, timeValFolder)
                m_Logger.PostEntry("Time validation error. View details in log for: " & moveLocPath, ILogger.logMsgType.logError, LOG_DATABASE)
                mail_msg = "Operator: " & m_xml_operator_Name
                mail_msg = mail_msg & Chr(13) & Chr(10) & "There was a time validation error with the following XML file: " & Chr(13) & Chr(10) & moveLocPath
                mail_msg = mail_msg & Chr(13) & Chr(10) & "Check the log for details.  " & Chr(13) & Chr(10)
                mail_msg = mail_msg + "Dataset filename and location: " + myDataXMLValidation.m_dataset_Path
                CreateMail(mail_msg, m_xml_operator_email, " - Time validation error.")
                Return False
            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_ENCOUNTERED_ERROR Then
                m_Logger.PostEntry(ModName & ": An error was encountered during the validation process.", ILogger.logMsgType.logNormal, LOG_DATABASE)
                m_Logger.PostEntry(ModName & ": An error was encountered during the validation process.", ILogger.logMsgType.logHealth, LOG_DATABASE)
                Return False
            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_WAIT_FOR_FILES Then
                Return False
            ElseIf xmlRslt = IXMLValidateStatus.XmlValidateStatus.XML_VALIDATE_NO_DATA Then
                moveLocPath = MoveXmlFile(xmlFilename, failureFolder)
                m_Logger.PostEntry(ModName & ": The dataset data is not available.", ILogger.logMsgType.logNormal, LOG_DATABASE)
                m_Logger.PostEntry(ModName & ": The dataset data is not available.", ILogger.logMsgType.logHealth, LOG_DATABASE)
                mail_msg = "Operator: " & m_xml_operator_Name
                mail_msg = mail_msg & Chr(13) & Chr(10) & "The dataset data is not available for capture and was not added to DMS for dataset: " & Chr(13) & Chr(10) & moveLocPath
                mail_msg = mail_msg & Chr(13) & Chr(10) & "Check the log for details.  " & Chr(13) & Chr(10)
                mail_msg = mail_msg + "Dataset not found in following location: " + myDataXMLValidation.m_dataset_Path
                m_db_Err_Msg = "The dataset data is not available for capture"
                rslt = myDataImportTask.GetDbErrorSolution(m_db_Err_Msg)
                If Not rslt Then
                    m_db_Err_Msg = ""
                End If
                CreateMail(mail_msg, m_xml_operator_email, " - Dataset not found.")
                Return False
            End If
        Catch ex As Exception
            m_Logger.PostError("Error validating Xml Data file", ex, LOG_DATABASE)
            Return False
        End Try

        Return True
    End Function

End Class
