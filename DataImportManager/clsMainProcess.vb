Imports System.Collections.Concurrent
Imports System.IO
Imports DataImportManager.clsGlobal
Imports System.Collections.Generic
Imports System.Net.Mail
Imports System.Runtime.InteropServices
Imports System.Text
Imports PRISM.Logging
Imports System.Windows.Forms
Imports System.Threading
Imports System.Threading.Tasks
Imports PRISM

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

    ''' <summary>
    ''' Keys in this dictionary are instrument names
    ''' Values are the number of datasets skipped for the given instrument
    ''' </summary>
    ''' <remarks></remarks>
    Private ReadOnly mInstrumentsToSkip As ConcurrentDictionary(Of String, Integer)

    Private mFailureCount As Integer

    ''' <summary>
    ''' Keys in this dictionary are semicolon separated e-mail addresses
    ''' Values are mail messages to send
    ''' </summary>
    ''' <remarks></remarks>
    Private ReadOnly mQueuedMail As ConcurrentDictionary(Of String, List(Of clsQueuedMail))

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

        mInstrumentsToSkip = New ConcurrentDictionary(Of String, Integer)(StringComparer.InvariantCultureIgnoreCase)

        mQueuedMail = New ConcurrentDictionary(Of String, List(Of clsQueuedMail))(StringComparer.InvariantCultureIgnoreCase)
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


        ' Everything worked
        Return True

    End Function

    ''' <summary>
    ''' Look for new XML files to process
    ''' </summary>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>Returns true even if no XML files are found</remarks>
    Public Function DoImport() As Boolean

        Try

            ' Verify an error hasn't left the the system in an odd state
            If DetectStatusFlagFile() Then
                Const statusMsg = "Flag file exists - auto-deleting it, then closing program"
                If TraceMode Then ShowTraceMessage(statusMsg)
                m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
                DeleteStatusFlagFile(m_Logger)

                If Not GetHostName().ToLower.StartsWith("monroe") Then
                    Return True
                End If

            End If

            Dim pendingWindowsUpdateMessage As String = String.Empty
            If clsWindowsUpdateStatus.ServerUpdatesArePending(DateTime.Now, pendingWindowsUpdateMessage) Then
                Dim warnMessage = "Monthly windows updates are pending; aborting check for new XML trigger files: " & pendingWindowsUpdateMessage

                If TraceMode Then
                    ShowTraceMessage(warnMessage)
                Else
                    Console.WriteLine(warnMessage)
                End If

                Return True
            End If

            ' Check to see if machine settings have changed
            If m_ConfigChanged Then
                If TraceMode Then ShowTraceMessage("Loading manager settings from the database")
                m_ConfigChanged = False
                If Not m_MgrSettings.LoadSettings(True) Then
                    If Not String.IsNullOrEmpty(m_MgrSettings.ErrMsg) Then
                        ' Manager has been deactivated, so report this
                        If TraceMode Then ShowTraceMessage(m_MgrSettings.ErrMsg)
                        m_Logger.PostEntry(m_MgrSettings.ErrMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
                    Else
                        ' Unknown problem reading config file
                        Const errMsg = "Unknown error re-reading config file"
                        If TraceMode Then ShowTraceMessage(errMsg)
                        m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                    End If

                    Exit Function
                End If
                m_FileWatcher.EnableRaisingEvents = True
            End If

            ' Check to see if excessive consecutive failures have occurred
            If mFailureCount > MAX_ERROR_COUNT Then
                ' More than MAX_ERROR_COUNT consecutive failures; there must be a generic problem, so exit
                Const errMsg = "Excessive task failures, disabling manager"
                If TraceMode Then ShowTraceMessage(errMsg)
                m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
                DisableManagerLocally()
            End If

            ' Check to see if the manager is still active
            If Not m_MgrActive Then
                If TraceMode Then ShowTraceMessage("Manager is inactive")
                m_Logger.PostEntry("Manager inactive", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
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

            Thread.Sleep(250)

            Return True

        Catch ex As Exception
            Dim errMsg As String = "Exception in clsMainProcess.DoImport(), " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
            Return False
        End Try

    End Function

    Private Sub DoDataImportTask(infoCache As DMSInfoCache)

        Dim result As ITaskParams.CloseOutType

        Dim delBadXmlFilesDays = CInt(m_MgrSettings.GetParam("deletebadxmlfiles"))
        Dim delGoodXmlFilesDays = CInt(m_MgrSettings.GetParam("deletegoodxmlfiles"))
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

            ' Send any queued mail
            If mQueuedMail.Count > 0 Then
                SendQueuedMail()
            End If

            For Each kvItem As KeyValuePair(Of String, Integer) In mInstrumentsToSkip
                Dim strMessage As String = "Skipped " & kvItem.Value & " dataset"
                If kvItem.Value <> 1 Then strMessage &= "s"
                strMessage &= " for instrument " & kvItem.Key & " due to network errors"
                m_Logger.PostEntry(strMessage, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
            Next

            ' Remove successful XML files older than x days
            DeleteXmlFiles(successFolder, delGoodXmlFilesDays)

            ' Remove failed XML files older than x days
            DeleteXmlFiles(failureFolder, delBadXmlFilesDays)

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
            m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
        End Try

    End Sub

    Private Function GetLogFileSharePath() As String

        Dim logFileName = m_MgrSettings.GetParam("logfilename")
        Return clsProcessXmlTriggerFile.GetLogFileSharePath(logFileName)

    End Function

    ''' <summary>
    ''' Retrieve the next chunk of items from a list
    ''' </summary>
    ''' <typeparam name="T"></typeparam>
    ''' <param name="sourceList">List of items to retrieve a chunk from; will be updated to remove the items in the returned list</param>
    ''' <param name="chunksize">Number of items to return</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function GetNextChunk(Of T)(ByRef sourceList As List(Of T), chunksize As Integer) As IEnumerable(Of T)
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
            .PreviewMode = PreviewMode
            .TraceMode = TraceMode
            .FailureFolder = failureFolder
            .SuccessFolder = successfolder
        End With

        Dim triggerProcessor = New clsProcessXmlTriggerFile(m_MgrSettings, mInstrumentsToSkip, infoCache, m_Logger, udtSettings)
        triggerProcessor.ProcessFile(currentFile)

        If triggerProcessor.QueuedMail.Count > 0 Then
            AddToMailQueue(triggerProcessor.QueuedMail)
        End If

    End Sub

    ''' <summary>
    ''' Add one or more mail messages to mQueuedMail
    ''' </summary>
    ''' <param name="newQueuedMail"></param>
    ''' <remarks></remarks>
    Private Sub AddToMailQueue(newQueuedMail As Dictionary(Of String, List(Of clsQueuedMail)))

        For Each newQueuedMessage In newQueuedMail
            Dim recipients = newQueuedMessage.Key

            Dim queuedMessages As List(Of clsQueuedMail) = Nothing
            If mQueuedMail.TryGetValue(recipients, queuedMessages) Then
                queuedMessages.AddRange(newQueuedMessage.Value)
            Else
                If Not mQueuedMail.TryAdd(recipients, newQueuedMessage.Value) Then
                    If mQueuedMail.TryGetValue(recipients, queuedMessages) Then
                        queuedMessages.AddRange(newQueuedMessage.Value)
                    End If
                End If
            End If

        Next

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
            ' There's a serious problem if the xfer directory can't be found!!!
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

    ''' <summary>
    ''' Send one digest e-mail to each unique combination of recipients
    ''' </summary>
    ''' <remarks>
    ''' Use of a digest e-mail reduces the e-mail spam sent by this tool
    ''' </remarks>
    Private Sub SendQueuedMail()

        Dim mailServer = m_MgrSettings.GetParam("smtpserver")
        If String.IsNullOrEmpty(mailServer) Then
            m_Logger.PostEntry("Manager parameter smtpserver is empty; cannot send mail", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
            Return
        End If

        For Each queuedRecipientList In mQueuedMail
            Dim recipients = queuedRecipientList.Key
            Dim messageCount = queuedRecipientList.Value.Count

            If messageCount < 1 Then
                ' Empty clsQueuedMail list; this should never happen
                m_Logger.PostEntry("Empty mail queue for recipients " & recipients & "; nothing to do", ILogger.logMsgType.logWarning, LOG_DATABASE)
                Continue For
            End If

            Dim queuedMail As clsQueuedMail = queuedRecipientList.Value(0)
            Dim mailToSend = queuedMail.Mail

            Dim subjectList = New SortedSet(Of String)
            Dim databaseErrors = New SortedSet(Of String)

            Dim mailBody = New StringBuilder()

            If Not String.IsNullOrWhiteSpace(queuedMail.InstrumentOperator) Then
                mailBody.AppendLine("Operator: " & queuedMail.InstrumentOperator)
                If messageCount > 1 Then
                    mailBody.AppendLine()
                End If
            End If

            mailBody.AppendLine(queuedMail.Mail.Body)

            CheckNewSubjectAndDatabaseMsg(queuedMail, mailBody, subjectList, databaseErrors)

            If messageCount > 1 Then

                ' Append the information for the additional messages
                For messageIndex = 1 To messageCount - 1
                    Dim additionalQueuedItem As clsQueuedMail = queuedRecipientList.Value(messageIndex)

                    mailBody.AppendLine()
                    mailBody.AppendLine("------------------------------------")
                    mailBody.AppendLine()
                    mailBody.AppendLine(additionalQueuedItem.Body)

                    CheckNewSubjectAndDatabaseMsg(additionalQueuedItem, mailBody, subjectList, databaseErrors)

                Next

                If subjectList.Count > 1 Then
                    ' Possibly update the subject of the e-mail
                    ' Subjects will typically only be:
                    ' "Data Import Manager - Database error."
                    '   or
                    ' "Data Import Manager - Database warning."
                    '  or
                    ' "Data Import Manager - Operator not defined."

                    ' If any of the subjects contains "error", use it for the mailsubject
                    For Each subject In From item In subjectList Where item.ToLower().Contains("error") Select item
                        mailToSend.Subject = subject
                        Exit For
                    Next

                End If

            End If

            If mailBody.ToString().Contains(clsProcessXmlTriggerFile.CHECK_THE_LOG_FOR_DETAILS) Then
                mailBody.AppendLine()
                mailBody.AppendLine("Log file location: " & GetLogFileSharePath())
            End If

            mailBody.AppendLine()
            mailBody.AppendLine("(NOTE: This message was sent from an account that is not monitored. If you have any questions, please reply to the list of recipients directly.)")

            mailToSend.Body = mailBody.ToString()

            If MailDisabled Then
                ShowTraceMessage("E-mail that would be sent:")
                ShowTraceMessage("  " & recipients)
                ShowTraceMessage("  " & mailToSend.Subject)
                ShowTraceMessage("  " & ControlChars.NewLine & mailToSend.Body)
            Else
                Dim smtp As New SmtpClient()
                smtp.Send(mailToSend)

                Threading.Thread.Sleep(100)
            End If
        Next

    End Sub

    Private Sub CheckNewSubjectAndDatabaseMsg(
      queuedMail As clsQueuedMail,
      mailBody As StringBuilder,
      subjectList As ISet(Of String),
      databaseErrors As ISet(Of String))

        If Not subjectList.Contains(queuedMail.Subject) Then
            subjectList.Add(queuedMail.Subject)
        End If

        If Not String.IsNullOrWhiteSpace(queuedMail.DatabaseErrorMsg) Then
            If Not databaseErrors.Contains(queuedMail.DatabaseErrorMsg) Then
                mailBody.AppendLine(queuedMail.DatabaseErrorMsg)
                databaseErrors.Add(queuedMail.DatabaseErrorMsg)
            End If
        End If

    End Sub

    Private Sub m_FileWatcher_Changed(sender As Object, e As FileSystemEventArgs) Handles m_FileWatcher.Changed
        m_ConfigChanged = True
        If m_DebugLevel > 3 Then
            m_Logger.PostEntry("Config file changed", ILogger.logMsgType.logDebug, LOG_LOCAL_ONLY)
        End If
        m_FileWatcher.EnableRaisingEvents = False  'Turn off change detection until current change has been acted upon
    End Sub

    Private Sub OnDbErrorEvent(message As String)
        If TraceMode Then ShowTraceMessage("Database error message: " & message)
        m_Logger.PostEntry(message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
    End Sub

    Private Function DeleteXmlFiles(folderPath As String, fileAgeDays As Integer) As Boolean

        Dim filedate As DateTime
        Dim daysDiff As Integer

        Dim workingDirectory = New DirectoryInfo(folderPath)

        ' Verify directory exists
        If Not workingDirectory.Exists Then
            ' There's a serious problem if the success/failure directory can't be found!!!

            Dim statusMsg As String = "Xml success/failure folder not found: " & folderPath
            If TraceMode Then ShowTraceMessage(statusMsg)

            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_DATABASE)
            Return False
        End If

        Dim deleteFailureCount = 0

        Try

            Dim xmlFiles As List(Of FileInfo) = workingDirectory.GetFiles("*.xml").ToList()

            For Each xmlFile In xmlFiles
                filedate = xmlFile.LastWriteTimeUtc
                daysDiff = DateTime.UtcNow.Subtract(filedate).Days
                If daysDiff > fileAgeDays Then
                    If PreviewMode Then
                        Console.WriteLine("Delete old file: " & xmlFile.FullName)
                    Else
                        Try
                            xmlFile.Delete()
                        Catch ex As Exception
                            Dim errMsg = "Error deleting old Xml Data file " & xmlFile.FullName & ": " & ex.Message
                            If TraceMode Then ShowTraceMessage(errMsg)
                            m_Logger.PostError(errMsg, ex, LOG_LOCAL_ONLY)
                            deleteFailureCount += 1
                        End Try

                    End If

                End If
            Next
        Catch ex As Exception
            Dim errMsg = "Error deleting old Xml Data files at " & folderPath
            If TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostError(errMsg, ex, LOG_DATABASE)
            Return False
        End Try

        If deleteFailureCount > 0 Then
            Dim errMsg =
                "Error deleting " & deleteFailureCount & " XML files at " & folderPath &
                " -- for a detailed list, see log file " & GetLogFileSharePath()

            If TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostEntry(errMsg, ILogger.logMsgType.logError, LOG_DATABASE)
        End If

        ' Everything must be OK if we got to here
        Return True

    End Function

    Private Sub DisableManagerLocally()

        If Not m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False") Then

            Dim statusMsg As String = "Error while disabling manager: " & m_MgrSettings.ErrMsg
            If TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
        End If

    End Sub

    Public Shared Sub ShowTraceMessage(strMessage As String)
        Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") & ": " & strMessage)
    End Sub

End Class
