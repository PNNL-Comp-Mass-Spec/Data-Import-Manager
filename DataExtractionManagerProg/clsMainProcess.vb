Imports System.Web.Mail
Imports System.IO
Imports DataImportManagerBase
Imports DataImportManagerBase.clsGlobal
Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports System.Collections.Specialized
Imports System.Text.RegularExpressions
Imports System.Xml
Imports System.Text.Encoding

Public Class clsMainProcess
	'**
	' This is the main class that does the following:

#Region "Member Variables"
    Private myMgrSettings As clsDataImportMgrSettings
    Private myDataImportTask As clsDataImportTask
    Private myLogger As ILogger
    Private Shared m_StartupClass As clsMainProcess
    Private m_IniFileChanged As Boolean = False
    Private WithEvents m_FileWatcher As New FileSystemWatcher
    Private m_IniFileName As String = "DataImportManager.xml"
    Private m_MgrActive As Boolean = True
    Private m_DebugLevel As Integer = 0
    Private m_message As String
    Private m_XmlFilesToLoad As New StringDictionary
    Dim m_ImportStatusCount As Integer = 0
#End Region

    Private Function GetIniFilePath(ByVal IniFileName As String) As String
        Dim fi As New FileInfo(Application.ExecutablePath)
        Return Path.Combine(fi.DirectoryName, IniFileName)
    End Function

    Public Sub New()

        Dim LogFile As String
        Dim ConnectStr As String
        Dim ModName As String
        Dim Fi As FileInfo

        Try
            'Load initial settings
            myMgrSettings = New clsDataImportMgrSettings(GetIniFilePath(m_IniFileName))
            m_MgrActive = CBool(myMgrSettings.GetParam("programcontrol", "mgractive"))
            m_DebugLevel = CInt(myMgrSettings.GetParam("programcontrol", "debuglevel"))

            ' create the object that will manage the logging
            LogFile = myMgrSettings.GetParam("logging", "logfilename")
            ConnectStr = myMgrSettings.GetParam("databasesettings", "connectionstring")
            ModName = myMgrSettings.GetParam("programcontrol", "modulename")
            myLogger = New clsQueLogger(New clsDBLogger(ModName, ConnectStr, LogFile))

            'Write the initial log and status entries
            myLogger.PostEntry("===== Started Data Import Manager V" & Application.ProductVersion & " =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

        Catch Err As System.Exception
            Throw New Exception("clsMainProcess.New(), " & Err.Message)
            Exit Sub
        End Try

        'Set up the FileWatcher to detect setup file changes
        Fi = New FileInfo(Application.ExecutablePath)
        m_FileWatcher.BeginInit()
        m_FileWatcher.Path = Fi.DirectoryName
        m_FileWatcher.IncludeSubdirectories = False
        m_FileWatcher.Filter = m_IniFileName
        m_FileWatcher.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
        m_FileWatcher.EndInit()
        m_FileWatcher.EnableRaisingEvents = True

    End Sub

    Shared Sub Main()

        Dim ErrMsg As String

        Try
            clsGlobal.AppFilePath = Application.ExecutablePath
            If IsNothing(m_StartupClass) Then
                m_StartupClass = New clsMainProcess
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
                myLogger.PostEntry("Flag file exists - unable to perform any further data import tasks", _
                ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                myLogger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                Exit Sub
            End If
            'Check to see if the machine settings have changed
            If m_IniFileChanged Then
                m_IniFileChanged = False
                If Not ReReadIniFile() Then
                    myLogger.PostEntry("Error re-reading ini file", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                    myLogger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                    Exit Sub
                End If
                m_FileWatcher.EnableRaisingEvents = True
            End If
            'Check to see if excessive consecutive failures have occurred
            If FailCount > 4 Then
                'More than 5 consecutive failures; there must be a generic problem, so exit
                myLogger.PostEntry("Multiple task failures, disabling manager", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                m_MgrActive = False
                myMgrSettings.SetParam("programcontrol", "mgractive", m_MgrActive.ToString)
                myMgrSettings.SaveSettings()
            End If
            'Check to see if the manager is still active
            If Not m_MgrActive Then
                myLogger.PostEntry("Manager inactive", ILogger.logMsgType.logNormal, True)
                myLogger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                Exit Sub
            End If

            'Check to see if there are any data import files ready
            DoDataImportTask()
            myLogger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
        Catch Err As System.Exception
            myLogger.PostEntry("clsMainProcess.DoImport(), " & Err.Message, ILogger.logMsgType.logError, True)
            myLogger.PostEntry("===== Closing Data Import Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
            Exit Sub
        End Try

    End Sub

    Private Sub DoDataImportTask()

        Dim result As ITaskParams.CloseOutType
        Dim rslt As Boolean
        Dim MachName As String = myMgrSettings.GetParam("programcontrol", "machname")
        Dim XferDir As String = myMgrSettings.GetParam("commonfileandfolderlocations", "xferdir")
        Dim runStatus As ITaskParams.CloseOutType = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS
        Dim DelBadXmlFilesDays As Integer = CInt(myMgrSettings.GetParam("programcontrol", "deletebadxmlfiles"))
        Dim DelGoodXmlFilesDays As Integer = CInt(myMgrSettings.GetParam("programcontrol", "deletegoodxmlfiles"))
        Dim successFolder As String = myMgrSettings.GetParam("commonfileandfolderlocations", "successfolder")
        Dim failureFolder As String = myMgrSettings.GetParam("commonfileandfolderlocations", "failurefolder")
        Dim x As Integer

        Try

            result = ScanXferDirectory()

            If result = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS And m_XmlFilesToLoad.Count > 0 Then
                'myLogger.PostEntry(MachName & ": Started Data import task ", ILogger.logMsgType.logNormal, LOG_DATABASE)

                CreateStatusFlagFile()    'Set status file for control of future runs


                ' create the object that will import the Data record
                '
                myDataImportTask = New clsDataImportTask(myMgrSettings, myLogger)
                ' request a new task using mgr parameters
                '
                'myLogger.PostEntry("Retrieving task", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                Application.DoEvents()

                'Add a delay
                Dim importDelay As String = myMgrSettings.GetParam("programcontrol", "importdelay")
                System.Threading.Thread.Sleep(CInt(importDelay) * 1000)

                Dim myEnumerator As IEnumerator = m_XmlFilesToLoad.GetEnumerator()
                Dim de As DictionaryEntry

                For Each de In m_XmlFilesToLoad
                    rslt = myDataImportTask.PostTask(CStr(de.Key))
                    MoveXmlFile(rslt, CStr(de.Key), successFolder, failureFolder)
                Next de
                m_XmlFilesToLoad.Clear()

            Else
                Exit Sub
            End If

            'Remove successful XML files older than x days
            DeleteXmlFiles(XferDir, DelBadXmlFilesDays, successFolder)

            'Remove failed XML files older than x days
            DeleteXmlFiles(XferDir, DelBadXmlFilesDays, failureFolder)

            ' If we got to here, then closeout the task as a success
            '
            DeleteStatusFlagFile(myLogger)
            FailCount = 0
            If runStatus = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS Then
                '                myDataImportTask.CloseTask(ITaskParams.CloseOutType.CLOSEOUT_SUCCESS, resultsFolder, myDataImportTask.GetParam("comment"))
                myLogger.PostEntry(MachName & ": Completed task ", ILogger.logMsgType.logNormal, LOG_DATABASE)
            End If

            'Copies the results to the transfer directory
            Dim ServerXferDir As String = myMgrSettings.GetParam("commonfileandfolderlocations", "xferdir")
            Dim filedate As DateTime


        Catch Err As System.Exception
            FailCount += 1
            myLogger.PostEntry("clsMainProcess.DoDataImportTask(), " & Err.Message, ILogger.logMsgType.logError, True)
            Exit Sub
        End Try

    End Sub

    Public Function ScanXferDirectory() As ITaskParams.CloseOutType

        'Copies the results to the transfer directory
        Dim ServerXferDir As String = myMgrSettings.GetParam("commonfileandfolderlocations", "xferdir")
        Dim filedate As DateTime

        'Verify transfer directory exists
        If Not Directory.Exists(ServerXferDir) Then
            'There's a serious problem is the xfer directory can't be found!!!
            myLogger.PostEntry("Xml transfer folder not found, job " & myMgrSettings.GetParam("jobNum"), _
             ILogger.logMsgType.logError, LOG_DATABASE)
            m_message = "Xml transfer folder not found"
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
            myLogger.PostError("Error loading Xml Data files", err, LOG_DATABASE)
            m_message = "Error loading Xml Data files"
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End Try
        m_XmlFilesToLoad.Values.GetEnumerator()
        'Everything must be OK if we got to here
        Return ITaskParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    Private Sub MoveXmlFile(ByVal Success As Boolean, ByVal xmlFile As String, ByVal successFolder As String, ByVal failureFolder As String)

        Dim Fi As FileInfo
        Dim xmlFilePath As String
        Dim xmlFileName As String
        Try
            If File.Exists(xmlFile) Then
                Fi = New FileInfo(xmlFile)
                xmlFilePath = Fi.DirectoryName
                xmlFileName = Fi.Name
                If Not Directory.Exists(xmlFilePath & "\" & successFolder) Then
                    Directory.CreateDirectory(xmlFilePath & "\" & successFolder)
                End If
                If Not Directory.Exists(xmlFilePath & "\" & failureFolder) Then
                    Directory.CreateDirectory(xmlFilePath & "\" & failureFolder)
                End If

                If Success Then
                    xmlFilePath = xmlFilePath & "\" & successFolder
                    File.Move(xmlFile, Path.Combine(xmlFilePath, xmlFileName))
                Else
                    xmlFilePath = xmlFilePath & "\" & failureFolder
                    File.Move(xmlFile, Path.Combine(xmlFilePath, xmlFileName))
                    CreateMail("There is a problem with the following XML file: " & Path.Combine(xmlFilePath, xmlFileName) & ".  Check the log for details.")
                End If
            End If
        Catch Err As System.Exception
            myLogger.PostEntry("MoveXmlFile, " & Err.Message, ILogger.logMsgType.logError, True)
        End Try

    End Sub

    Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles m_FileWatcher.Changed
        m_IniFileChanged = True
        m_FileWatcher.EnableRaisingEvents = False  'Turn off change detection until current change has been acted upon
    End Sub

    Private Function ReReadIniFile() As Boolean

        'Re-read the ini file that may have changed
        'Note: Assumes log file and module name entries in ini file don't get changed

        If Not myMgrSettings.LoadSettings Then
            myLogger.PostEntry("Error reloading settings file", ILogger.logMsgType.logError, True)
            Return False
        End If

        'NOTE: Debug level has implied update because new instances of resource retrieval and analysis tool
        '	classes retrieve the debug level from MyMgrSettings in the constructor

        m_MgrActive = CBool(myMgrSettings.GetParam("programcontrol", "mgractive"))
        m_DebugLevel = CInt(myMgrSettings.GetParam("programcontrol", "debuglevel"))
        'NOTE: Debug level implied update because new instances of resource retrieval and analysis tool
        '	classes retrieve the debug level from MyMgrSettings in the constructor
        Return True

    End Function

    Function CreateMail(ByVal mailMsg As String) As Boolean
        Dim report As String
        Dim beginStr As String
        Dim titleStr As String
        Dim ErrMsg As String
        Dim msg As New MailMessage
        Dim reportName As String
        Dim enableEmail As Boolean

        enableEmail = CBool(myMgrSettings.GetParam("programcontrol", "enableemail"))
        If enableEmail Then

            msg.BodyEncoding = ASCII
            msg.BodyFormat = MailFormat.Html
            msg.From = myMgrSettings.GetParam("programcontrol", "from")
            msg.To = myMgrSettings.GetParam("programcontrol", "to")
            msg.Subject = myMgrSettings.GetParam("programcontrol", "subject")
            msg.Body = mailMsg & Chr(13) & Chr(10) & Chr(13) & Chr(10) & " If log message is ""could not access CDO.Message object"", then make sure smtp server is valid."
            Try
                SmtpMail.SmtpServer = myMgrSettings.GetParam("programcontrol", "smtpserver")
                SmtpMail.Send(msg)
            Catch Ex As Exception
                ErrMsg = "Exception: " & Ex.Message & vbCrLf & vbCrLf & "Sending mail message."
                myLogger.PostEntry("Error creating email message", ILogger.logMsgType.logError, True)
            End Try
        End If

    End Function

    Private Function DeleteXmlFiles(ByVal FileDirectory As String, ByVal NoDays As Integer, ByVal subFolder As String) As ITaskParams.CloseOutType

        Dim filedate As DateTime
        Dim daysDiff As Integer
        Dim workDirectory As String

        workDirectory = FileDirectory & "\" & subFolder
        'Verify directory exists
        If Not Directory.Exists(workDirectory) Then
            'There's a serious problem if the success/failure directory can't be found!!!
            myLogger.PostEntry("Xml success/failure folder not found.", ILogger.logMsgType.logError, LOG_DATABASE)
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
            myLogger.PostError("Error deleting Xml Data files", err, LOG_DATABASE)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Everything must be OK if we got to here
        Return ITaskParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
