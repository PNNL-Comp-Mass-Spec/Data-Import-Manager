Imports System.Collections.Concurrent
Imports System.IO
Imports DataImportManager.clsGlobal
Imports System.Collections.Generic
Imports System.Runtime.InteropServices
Imports PRISM.Logging
Imports System.Windows.Forms
Imports System.Threading
Imports System.Threading.Tasks

Public Class clsMainProcess

#Region "Constants"
    Private Const EMERG_LOG_FILE As String = "DataImportMan_log.txt"
    Private Const MAX_ERROR_COUNT As Integer = 4
#End Region

#Region "Member Variables"
    Private m_MgrSettings As clsMgrSettings

    Private m_Logger As ILogger
    Private m_ConfigChanged As Boolean = False
    Private WithEvents m_FileWatcher As New FileSystemWatcher
    Private m_MgrActive As Boolean = True
    Private m_DebugLevel As Integer = 0
    Public m_db_Err_Msg As String

    ' Keys in this dictionary are instrument names
    ' Values are the number of datasets skipped for the given instrument
    Private m_InstrumentsToSkip As ConcurrentDictionary(Of String, Integer)

    Private mFailureCount As Integer

#End Region

#Region "Auto Properties"
    Public Property MailDisabled As Boolean
    Public Property TraceMode As Boolean
    Public Property PreviewMode As Boolean
#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="blnTraceMode"></param>
    ''' <remarks></remarks>
    Public Sub New(blnTraceMode As Boolean)
        TraceMode = blnTraceMode
    End Sub

    Public Function InitMgr() As Boolean

        ' Get the manager settings
        Try
            m_MgrSettings = New clsMgrSettings(EMERG_LOG_FILE)
            If m_MgrSettings.ManagerDeactivated Then
                If TraceMode Then ShowTraceMessage("m_MgrSettings.ManagerDeactivated = True")
                Return False
            End If
        Catch ex As Exception
            If TraceMode Then ShowTraceMessage("Exception instantiating m_MgrSettings: " & ex.Message)
            Throw New Exception("InitMgr, " & ex.Message)
            ' Failures are logged by clsMgrSettings to local emergency log file
        End Try

        Dim connectionstring = m_MgrSettings.GetParam("connectionstring")

        Dim FInfo = New FileInfo(GetExePath())
        Try
            ' Load initial settings
            m_MgrActive = CBool(m_MgrSettings.GetParam("mgractive"))
            m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

            ' create the object that will manage the logging
            Dim logFilePath = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("logfilename"))

            ' Make sure the log folder exists
            Try
                Dim fiLogFile = New FileInfo(logFilePath)
                If Not Directory.Exists(fiLogFile.DirectoryName) Then
                    Directory.CreateDirectory(fiLogFile.DirectoryName)
                End If
            Catch ex2 As Exception
                Console.WriteLine("Error checking for valid directory for Logfile: " & logFilePath)
            End Try

            Dim moduleName = m_MgrSettings.GetParam("modulename")
            m_Logger = New clsQueLogger(New clsDBLogger(moduleName, connectionstring, logFilePath))

            ' Write the initial log and status entries
            m_Logger.PostEntry("===== Started Data Import Manager V" & Application.ProductVersion & " =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

        Catch ex As Exception
            If TraceMode Then ShowTraceMessage("Exception loading initial settings: " & ex.Message)
            Throw New Exception("InitMgr, " & ex.Message)
        End Try

        ' Setup the logger
        Dim LogFileName As String = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("logfilename"))
        Dim DbLogger As New clsDBLogger
        DbLogger.LogFilePath = LogFileName
        DbLogger.ConnectionString = m_MgrSettings.GetParam("connectionstring")
        DbLogger.ModuleName = m_MgrSettings.GetParam("modulename")
        m_Logger = New clsQueLogger(DbLogger)

        ' Set up the FileWatcher to detect setup file changes
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

        ' Get the debug level
        m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

        m_InstrumentsToSkip = New ConcurrentDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

        ' Everything worked
        Return True

    End Function

    Public Function DoImport() As Boolean

        Try

            ' Verify an error hasn't left the the system in an odd state
            If DetectStatusFlagFile() Then
                Const statusMsg As String = "Flag file exists - auto-deleting it, then closing program"
                If TraceMode Then ShowTraceMessage(statusMsg)
                m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
                DeleteStatusFlagFile(m_Logger)

                If Not GetHostName().ToLower.StartsWith("monroe") Then
                    Exit Function
                End If

            End If

            ' Check to see if machine settings have changed
            If m_ConfigChanged Then
                If TraceMode Then ShowTraceMessage("Loading manager settings from the database")
                m_ConfigChanged = False
                If Not m_MgrSettings.LoadSettings(True) Then
                    If Not String.IsNullOrEmpty(m_MgrSettings.ErrMsg) Then
                        ' Manager has been deactivated, so report this
                        If TraceMode Then ShowTraceMessage(m_MgrSettings.ErrMsg)
                        m_Logger.PostEntry(m_MgrSettings.ErrMsg, ILogger.logMsgType.logWarning, True)
                    Else
                        ' Unknown problem reading config file
                        Const errMsg As String = "Unknown error re-reading config file"
                        If TraceMode Then ShowTraceMessage(errMsg)
                        m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, True)
                    End If

                    Exit Function
                End If
                m_FileWatcher.EnableRaisingEvents = True
            End If

            ' Check to see if excessive consecutive failures have occurred
            If mFailureCount > MAX_ERROR_COUNT Then
                ' More than MAX_ERROR_COUNT consecutive failures; there must be a generic problem, so exit
                Const errMsg As String = "Excessive task failures, disabling manager"
                If TraceMode Then ShowTraceMessage(errMsg)
                m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                DisableManagerLocally()
            End If

            ' Check to see if the manager is still active
            If Not m_MgrActive Then
                If TraceMode Then ShowTraceMessage("Manager is inactive")
                m_Logger.PostEntry("Manager inactive", ILogger.logMsgType.logNormal, True)
                Exit Function
            End If

            Dim connectionString = m_MgrSettings.GetParam("connectionstring")
            Dim infoCache As DMSInfoCache

            Try
                infoCache = New DMSInfoCache(connectionString, m_Logger, TraceMode)
            Catch ex As Exception
                Dim errMsg = "Unable to connect to the database using " & connectionString
                If TraceMode Then ShowTraceMessage(errMsg)
                m_Logger.PostError(errMsg, ex, LOG_LOCAL_ONLY)
                Return False
            End Try

            AddHandler infoCache.DBErrorEvent, New DMSInfoCache.DBErrorEventEventHandler(AddressOf OnDbErrorEvent)

            ' Check to see if there are any data import files ready
            DoDataImportTask(infoCache)

            infoCache.CloseDatabaseConnection()

            Return True

        Catch ex As Exception
            Dim errMsg As String = "Exception in clsMainProcess.DoImport(), " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, True)
            Return False
        End Try

    End Function

    Private Sub DoDataImportTask(infoCache As DMSInfoCache)

        Dim result As ITaskParams.CloseOutType

        Dim DelBadXmlFilesDays As Integer = CInt(m_MgrSettings.GetParam("deletebadxmlfiles"))
        Dim DelGoodXmlFilesDays As Integer = CInt(m_MgrSettings.GetParam("deletegoodxmlfiles"))
        Dim successFolder As String = m_MgrSettings.GetParam("successfolder")
        Dim failureFolder As String = m_MgrSettings.GetParam("failurefolder")

        Try
            Dim xmlFilesToImport As List(Of FileInfo) = Nothing
            result = ScanXferDirectory(xmlFilesToImport)

            If result = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS And xmlFilesToImport.Count > 0 Then

                CreateStatusFlagFile()    'Set status file for control of future runs

                ' Add a delay
                Dim importDelayText As String = m_MgrSettings.GetParam("importdelay")
                Dim importDelay As Integer
                If Not Integer.TryParse(importDelayText, importDelay) Then
                    Dim statusMsg = "Manager parameter ImportDelay was not numeric: " & importDelayText
                    If TraceMode Then ShowTraceMessage(statusMsg)
                    m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                    importDelay = 2
                End If

                If GetHostName().ToLower().StartsWith("monroe") Then
                    Console.WriteLine("Changing importDelay from " & importDelay & " seconds to 1 second since host starts with Monroe")
                    importDelay = 1
                ElseIf PreviewMode Then
                    Console.WriteLine("Changing importDelay from " & importDelay & " seconds to 1 second since PreviewMode is enabled")
                    importDelay = 1
                End If

                If TraceMode Then ShowTraceMessage("ImportDelay, sleep for " & importDelay & " seconds")
                Thread.Sleep(importDelay * 1000)

                ' Load information from DMS
                infoCache.LoadDMSInfo()

                ' Randomize order of files in m_XmlFilesToLoad
                xmlFilesToImport.Shuffle()

                ' Process the files in parallel, in groups of 50 at a time

                Do
                    Dim currentChunk As IEnumerable(Of FileInfo) = GetNextChunk(xmlFilesToImport, 50)

                    If currentChunk.Count > 1 Then
                        m_Logger.PostEntry("Processing " & currentChunk.Count & " XML files in parallel", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
                    End If

                    Parallel.ForEach(currentChunk, Sub(currentFile)
                                                       ProcessOneFile(currentFile, successFolder, failureFolder, infoCache)
                                                   End Sub)

                Loop While xmlFilesToImport.Count > 0

            Else
                If m_DebugLevel > 4 Or TraceMode Then
                    Dim statusMsg = "No Data Files to import"
                    ShowTraceMessage(statusMsg)
                    m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logDebug, LOG_LOCAL_ONLY)
                End If
                Exit Sub
            End If

            For Each kvItem As KeyValuePair(Of String, Integer) In m_InstrumentsToSkip
                Dim strMessage As String = "Skipped " & kvItem.Value & " dataset"
                If kvItem.Value <> 1 Then strMessage &= "s"
                strMessage &= " for instrument " & kvItem.Key & " due to network errors"
                m_Logger.PostEntry(strMessage, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
            Next

            ' Remove successful XML files older than x days
            DeleteXmlFiles(successFolder, DelGoodXmlFilesDays)

            ' Remove failed XML files older than x days
            DeleteXmlFiles(failureFolder, DelBadXmlFilesDays)

            ' If we got to here, then closeout the task as a success
            '
            DeleteStatusFlagFile(m_Logger)
            mFailureCount = 0

            Dim completionMsg = "Completed task"
            If TraceMode Then ShowTraceMessage(completionMsg)
            m_Logger.PostEntry(completionMsg, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

        Catch ex As Exception
            mFailureCount += 1

            Dim errMsg = "Exception in clsMainProcess.DoDataImportTask(), " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, True)
        End Try

    End Sub
    
    ''' <summary>
    ''' Retrieve the next chunk of items from a list
    ''' </summary>
    ''' <typeparam name="T"></typeparam>
    ''' <param name="sourceList">List of items to retrieve a chunk from; will be updated to remove the items in the returned list</param>
    ''' <param name="chunksize">Number of items to return</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function GetNextChunk(Of T)(ByRef sourceList As List(Of T), chunksize As Integer) As IEnumerable(Of T)
        If chunksize < 1 Then chunksize = 1
        If sourceList.Count < 1 Then
            Return New List(Of T)
        End If

        Dim nextChunk As IEnumerable(Of T)

        If chunksize >= sourceList.Count Then
            nextChunk = sourceList.Take(sourceList.Count).ToList()
            sourceList = New List(Of T)
        Else
            nextChunk = sourceList.Take(chunksize).ToList()
            Dim remainingItems = sourceList.Skip(chunksize)
            sourceList = remainingItems.ToList()
        End If

        Return nextChunk

    End Function

    Private Sub ProcessOneFile(currentFile As FileInfo, successfolder As String, failureFolder As String, infoCache As DMSInfoCache)

        Dim objRand As New Random()

        ' Delay for anywhere between 1 to 15 seconds so that the tasks don't all fire at once
        Dim waitSeconds = objRand.Next(1, 15)
        Dim dtStartTime = DateTime.UtcNow
        While DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds < waitSeconds
            Thread.Sleep(100)
        End While

        ' Validate the xml file

        Dim udtSettings = New clsProcessXmlTriggerFile.udtXmlProcSettingsType
        With udtSettings
            .PreviewMode = PreviewMode
            .DebugLevel = m_DebugLevel
            .MailDisabled = MailDisabled
            .PreviewMode = PreviewMode
            .TraceMode = TraceMode
            .FailureFolder = failureFolder
            .SuccessFolder = successfolder
        End With

        Dim triggerProcessor = New clsProcessXmlTriggerFile(m_MgrSettings, m_InstrumentsToSkip, infoCache, m_Logger, udtSettings)
        triggerProcessor.ProcessFile(currentFile)

    End Sub

    Public Function ScanXferDirectory(<Out()> ByRef xmlFilesToImport As List(Of FileInfo)) As ITaskParams.CloseOutType

        ' Copies the results to the transfer directory
        Dim serverXferDir As String = m_MgrSettings.GetParam("xferdir")

        If String.IsNullOrWhiteSpace(serverXferDir) Then
            m_Logger.PostEntry("Manager parameter xferdir is empty (" + GetHostName() + ")", ILogger.logMsgType.logError, LOG_DATABASE)
            xmlFilesToImport = New List(Of FileInfo)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim diXferDirectory = New DirectoryInfo(serverXferDir)

        ' Verify transfer directory exists
        If Not diXferDirectory.Exists Then
            ' There's a serious problem is the xfer directory can't be found!!!
            Dim statusMsg As String = "Xml transfer folder not found: " & serverXferDir
            If TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_DATABASE)
            xmlFilesToImport = New List(Of FileInfo)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Load all the Xml File names and dates in the transfer directory into a string dictionary
        Try
            If TraceMode Then ShowTraceMessage("Finding XML files at " & serverXferDir)

            xmlFilesToImport = diXferDirectory.GetFiles("*.xml").ToList()

        Catch ex As Exception
            Dim errMsg = "Error loading Xml Data files from " & serverXferDir
            If TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostError(errMsg, ex, LOG_DATABASE)
            xmlFilesToImport = New List(Of FileInfo)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' Everything must be OK if we got to here
        Return ITaskParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Sub m_FileWatcher_Changed(sender As Object, e As FileSystemEventArgs) Handles m_FileWatcher.Changed
        m_ConfigChanged = True
        If m_DebugLevel > 3 Then
            m_Logger.PostEntry("Config file changed", ILogger.logMsgType.logDebug, True)
        End If
        m_FileWatcher.EnableRaisingEvents = False  'Turn off change detection until current change has been acted upon
    End Sub

    Private Sub OnDbErrorEvent(message As String)
        If TraceMode Then ShowTraceMessage("Database error message: " & message)
        m_Logger.PostEntry(message, ILogger.logMsgType.logError, True)
    End Sub

    Private Function DeleteXmlFiles(FileDirectory As String, NoDays As Integer) As Boolean

        Dim filedate As DateTime
        Dim daysDiff As Integer
        Dim workDirectory As String

        workDirectory = FileDirectory
        ' Verify directory exists
        If Not Directory.Exists(workDirectory) Then
            ' There's a serious problem if the success/failure directory can't be found!!!

            Dim statusMsg As String = "Xml success/failure folder not found: " & workDirectory
            If TraceMode Then ShowTraceMessage(statusMsg)

            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_DATABASE)
            Return False
        End If

        ' Load all the Xml File names and dates in the transfer directory into a string dictionary
        Try
            Dim XmlFilesToDelete() As String = Directory.GetFiles(Path.Combine(workDirectory, workDirectory))
            For Each XmlFile As String In XmlFilesToDelete
                filedate = File.GetLastWriteTimeUtc(XmlFile)
                daysDiff = DateTime.UtcNow.Subtract(filedate).Days
                If daysDiff > NoDays Then
                    File.Delete(XmlFile)
                End If
            Next
        Catch ex As Exception
            Dim errMsg = "Error deleting old Xml Data files at " & FileDirectory
            If TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostError(errMsg, ex, LOG_DATABASE)
            Return False
        End Try

        ' Everything must be OK if we got to here
        Return True

    End Function

    Private Sub DisableManagerLocally()

        If Not m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False") Then

            Dim statusMsg As String = "Error while disabling manager: " & m_MgrSettings.ErrMsg
            If TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logError, True)
        End If

    End Sub

    Public Shared Sub ShowTraceMessage(strMessage As String)
        Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") & ": " & strMessage)
    End Sub

End Class
