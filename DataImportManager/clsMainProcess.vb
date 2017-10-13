Imports System.Collections.Concurrent
Imports System.IO
Imports DataImportManager.clsGlobal
Imports System.Net.Mail
Imports System.Runtime.InteropServices
Imports System.Text
Imports System.Windows.Forms
Imports System.Threading
Imports System.Threading.Tasks
Imports PRISM
Imports DataImportManager.clsValidationErrorSummary

Public Class clsMainProcess

#Region "Constants"
    Private Const EMERG_LOG_FILE As String = "DataImportMan_log.txt"
    Private Const MAX_ERROR_COUNT As Integer = 4
#End Region

#Region "Member Variables"
    Private m_MgrSettings As clsMgrSettings

    Private m_Logger As ILogger
    Private m_ConfigChanged As Boolean = False

    Private m_FileWatcher As FileSystemWatcher

    Private m_MgrActive As Boolean = True

    Private m_DebugLevel As Integer = 0

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
    Private ReadOnly mQueuedMail As ConcurrentDictionary(Of String, ConcurrentBag(Of clsQueuedMail))

#End Region

#Region "Auto Properties"
    Public Property IgnoreInstrumentSourceErrors As Boolean
    Public Property MailDisabled As Boolean
    Public Property PreviewMode As Boolean
    Public Property TraceMode As Boolean
#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="blnTraceMode"></param>
    ''' <remarks></remarks>
    Public Sub New(blnTraceMode As Boolean)
        TraceMode = blnTraceMode

        mInstrumentsToSkip = New ConcurrentDictionary(Of String, Integer)(StringComparer.OrdinalIgnoreCase)

        mQueuedMail = New ConcurrentDictionary(Of String, ConcurrentBag(Of clsQueuedMail))(StringComparer.OrdinalIgnoreCase)
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
            Throw New Exception("InitMgr, " & ex.Message, ex)
            ' Failures are logged by clsMgrSettings to local emergency log file
        End Try

        Dim connectionString = m_MgrSettings.GetParam("connectionstring")

        Dim exeFile = New FileInfo(GetExePath())
        Try
            ' Load initial settings
            m_MgrActive = CBool(m_MgrSettings.GetParam("mgractive"))
            m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

            ' create the object that will manage the logging
            Dim logFilePath = Path.Combine(exeFile.DirectoryName, m_MgrSettings.GetParam("logfilename"))

            ' Make sure the log folder exists
            Try
                Dim fiLogFile = New FileInfo(logFileBaseName)
                If Not Directory.Exists(fiLogFile.DirectoryName) Then
                    Directory.CreateDirectory(fiLogFile.DirectoryName)
                End If
            Catch ex2 As Exception
                Console.WriteLine("Error checking for valid directory for logging: " & logFileBaseName)
            End Try

            Dim moduleName = m_MgrSettings.GetParam("modulename")
            m_Logger = New clsQueLogger(New clsDBLogger(moduleName, connectionstring, logFilePath))

            ' Write the initial log and status entries
            m_Logger.PostEntry("===== Started Data Import Manager V" & Application.ProductVersion & " =====", logMsgType.logNormal, LOG_LOCAL_ONLY)

        Catch ex As Exception
            If TraceMode Then ShowTraceMessage("Exception loading initial settings: " & ex.Message)
            Throw New Exception("InitMgr, " & ex.Message, ex)
        End Try

        ' Set up the FileWatcher to detect setup file changes
        m_FileWatcher = New FileSystemWatcher()
        With m_FileWatcher
            .BeginInit()
            .Path = exeFile.DirectoryName
            .IncludeSubdirectories = False
            .Filter = m_MgrSettings.GetParam("configfilename")
            .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
            .EndInit()
            .EnableRaisingEvents = True
        End With

        AddHandler m_FileWatcher.Changed, AddressOf FileWatcher_Changed

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
                m_Logger.PostEntry(statusMsg, logMsgType.logWarning, LOG_LOCAL_ONLY)
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
                        m_Logger.PostEntry(m_MgrSettings.ErrMsg, logMsgType.logWarning, LOG_LOCAL_ONLY)
                    Else
                        ' Unknown problem reading config file
                        Const errMsg = "Unknown error re-reading config file"
                        If TraceMode Then ShowTraceMessage(errMsg)
                        m_Logger.PostEntry(errMsg, logMsgType.logError, LOG_LOCAL_ONLY)
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
                m_Logger.PostEntry(errMsg, logMsgType.logError, LOG_LOCAL_ONLY)
                DisableManagerLocally()
            End If

            ' Check to see if the manager is still active
            If Not m_MgrActive Then
                If TraceMode Then ShowTraceMessage("Manager is inactive")
                m_Logger.PostEntry("Manager inactive", logMsgType.logNormal, LOG_LOCAL_ONLY)
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
            m_Logger.PostEntry(errMsg & "; " & GetExceptionStackTrace(ex), logMsgType.logError, LOG_DATABASE)
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
                    m_Logger.PostEntry(statusMsg, logMsgType.logNormal, LOG_LOCAL_ONLY)
                    importDelay = 2
                End If

                If GetHostName().ToLower().StartsWith("monroe") Then
                    ' Console.WriteLine("Changing importDelay from " & importDelay & " seconds to 1 second since host starts with Monroe")
                    importDelay = 1
                ElseIf PreviewMode Then
                    ' Console.WriteLine("Changing importDelay from " & importDelay & " seconds to 1 second since PreviewMode is enabled")
                    importDelay = 1
                End If

                If TraceMode Then ShowTraceMessage("ImportDelay, sleep for " & importDelay & " seconds")
                Thread.Sleep(importDelay * 1000)

                ' Load information from DMS
                infoCache.LoadDMSInfo()

                ' Randomize order of files in m_XmlFilesToLoad
                xmlFilesToImport.Shuffle()

                If TraceMode Then ShowTraceMessage("Processing " & xmlFilesToImport.Count & " XML files")

                ' Process the files in parallel, in groups of 50 at a time
                '
                Do
                    Dim currentChunk As IEnumerable(Of FileInfo) = GetNextChunk(xmlFilesToImport, 50)

                    If currentChunk.Count > 1 Then
                        m_Logger.PostEntry("Processing " & currentChunk.Count & " XML files in parallel", logMsgType.logNormal, LOG_LOCAL_ONLY)
                    End If

                    Parallel.ForEach(currentChunk, Sub(currentFile)
                                                       ProcessOneFile(currentFile, successFolder, failureFolder, infoCache)
                                                   End Sub)

                Loop While xmlFilesToImport.Count > 0

            Else
                If m_DebugLevel > 4 Or TraceMode Then
                    Dim statusMsg = "No Data Files to import"
                    ShowTraceMessage(statusMsg)
                    m_Logger.PostEntry(statusMsg, logMsgType.logDebug, LOG_LOCAL_ONLY)
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
                m_Logger.PostEntry(strMessage, logMsgType.logNormal, LOG_LOCAL_ONLY)
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
            m_Logger.PostEntry(completionMsg, logMsgType.logNormal, LOG_LOCAL_ONLY)

        Catch ex As Exception
            mFailureCount += 1

            Dim errMsg = "Exception in clsMainProcess.DoDataImportTask(), " & ex.Message
            If TraceMode Then ShowTraceMessage(errMsg)

            m_Logger.PostEntry(errMsg & "; " & GetExceptionStackTrace(ex), logMsgType.logError, LOG_LOCAL_ONLY)
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
            .DebugLevel = m_DebugLevel
            .IgnoreInstrumentSourceErrors = IgnoreInstrumentSourceErrors
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
    Private Sub AddToMailQueue(newQueuedMail As ConcurrentDictionary(Of String, ConcurrentBag(Of clsQueuedMail)))

        For Each newQueuedMessage In newQueuedMail
            Dim recipients = newQueuedMessage.Key

            Dim queuedMessages As ConcurrentBag(Of clsQueuedMail) = Nothing
            If mQueuedMail.TryGetValue(recipients, queuedMessages) Then
                For Each msg In newQueuedMessage.Value
                    queuedMessages.Add(msg)
                Next

            Else
                If Not mQueuedMail.TryAdd(recipients, newQueuedMessage.Value) Then
                    If mQueuedMail.TryGetValue(recipients, queuedMessages) Then
                        For Each msg In newQueuedMessage.Value
                            queuedMessages.Add(msg)
                        Next
                    End If
                End If
            End If

        Next

    End Sub

    Public Function ScanXferDirectory(<Out> ByRef xmlFilesToImport As List(Of FileInfo)) As ITaskParams.CloseOutType

        ' Copies the results to the transfer directory
        Dim serverXferDir As String = m_MgrSettings.GetParam("xferdir")

        If String.IsNullOrWhiteSpace(serverXferDir) Then
            m_Logger.PostEntry("Manager parameter xferdir is empty (" + GetHostName() + ")", logMsgType.logError, LOG_DATABASE)
            xmlFilesToImport = New List(Of FileInfo)
            Return ITaskParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim diXferDirectory = New DirectoryInfo(serverXferDir)

        ' Verify transfer directory exists
        If Not diXferDirectory.Exists Then
            ' There's a serious problem if the xfer directory can't be found!!!
            Dim statusMsg As String = "Xml transfer folder not found: " & serverXferDir
            If TraceMode Then ShowTraceMessage(statusMsg)

            m_Logger.PostEntry(statusMsg, logMsgType.logError, LOG_DATABASE)
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

        Dim currentTask = "Initializing"
        Dim msg As String

        Try

            currentTask = "Get smptserver param"
            Dim mailServer = m_MgrSettings.GetParam("smtpserver")
            If String.IsNullOrEmpty(mailServer) Then
                msg = "Manager parameter smtpserver is empty; cannot send mail"
                If TraceMode Then ShowTraceMessage(msg)
                m_Logger.PostEntry(msg, logMsgType.logError, LOG_LOCAL_ONLY)
                Return
            End If

            currentTask = "Check for new log file"

            Dim logFileName = "MailLog_" & DateTime.Now.ToString("yyyy-MM") & ".txt"
            Dim mailLogFile As FileInfo

            If String.IsNullOrWhiteSpace(m_Logger.CurrentLogFilePath) Then
                Dim exeFile = New FileInfo(GetExePath())
                mailLogFile = New FileInfo(Path.Combine(exeFile.DirectoryName, "Logs", logFileName))
            Else
                currentTask = "Get current log file path"
                Dim currentLogFile = New FileInfo(m_Logger.CurrentLogFilePath)

                currentTask = "Get new log file path"
                mailLogFile = New FileInfo(Path.Combine(currentLogFile.Directory.FullName, logFileName))
            End If

            Dim newLogFile = Not mailLogFile.Exists()

            currentTask = "Initialize stringbuilder"

            Dim mailContentPreview = New StringBuilder()

            If newLogFile Then
                If TraceMode Then ShowTraceMessage("Creating new mail log file " & mailLogFile.FullName)
            Else
                If TraceMode Then ShowTraceMessage("Appending to mail log file " & mailLogFile.FullName)
            End If

            currentTask = "Create the mail logger"
            Using mailLogger = New StreamWriter(New FileStream(mailLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))

                mailLogger.AutoFlush = True

                currentTask = "Iterate over mQueuedMail"
                For Each queuedMailContainer In mQueuedMail
                    Dim recipients = queuedMailContainer.Key
                    Dim messageCount = queuedMailContainer.Value.Count

                    If messageCount < 1 Then
                        If TraceMode Then ShowTraceMessage("Empty clsQueuedMail list; this should never happen")
                        m_Logger.PostEntry("Empty mail queue for recipients " & recipients & "; nothing to do", logMsgType.logWarning, LOG_DATABASE)
                        Continue For
                    End If

                    currentTask = "Get first queued mail"
                    Dim firstQueuedMail As clsQueuedMail = queuedMailContainer.Value(0)

                    If firstQueuedMail Is Nothing Then
                        msg = "firstQueuedMail item is null in SendQueuedMail"
                        If TraceMode Then ShowTraceMessage(msg)
                        m_Logger.PostEntry(msg, logMsgType.logError, LOG_DATABASE)

                        Dim defaultRecipients = m_MgrSettings.GetParam("to")
                        firstQueuedMail = New clsQueuedMail("Unknown Operator", defaultRecipients, "Exception", New List(Of clsValidationError))
                    End If

                    ' Create the mail message
                    Dim mailToSend As New MailMessage()

                    ' Set the addresses
                    mailToSend.From = New MailAddress(m_MgrSettings.GetParam("from"))

                    Dim mailRecipientsList = firstQueuedMail.Recipients.Split(";"c).Distinct().ToList()
                    For Each emailAddress As String In mailRecipientsList
                        mailToSend.To.Add(emailAddress)
                    Next

                    mailToSend.Subject = firstQueuedMail.Subject

                    Dim subjectList = New SortedSet(Of String)
                    Dim databaseErrorMsgs = New SortedSet(Of String)
                    Dim instrumentFilePaths = New SortedSet(Of String)

                    Dim mailBody = New StringBuilder()

                    Dim statusMsg As String
                    If messageCount = 1 Then
                        statusMsg = "E-mailing " & recipients & " regarding " & firstQueuedMail.InstrumentDatasetPath
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_Logger.PostEntry(statusMsg, logMsgType.logDebug, LOG_LOCAL_ONLY)
                    Else
                        statusMsg = "E-mailing " & recipients & " regarding " & messageCount & " errors"
                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_Logger.PostEntry(statusMsg, logMsgType.logDebug, LOG_LOCAL_ONLY)
                    End If

                    If Not String.IsNullOrWhiteSpace(firstQueuedMail.InstrumentOperator) Then
                        mailBody.AppendLine("Operator: " & firstQueuedMail.InstrumentOperator)
                        If messageCount > 1 Then
                            mailBody.AppendLine()
                        End If
                    End If

                    ' Summarize the validation errors
                    Dim summarizedErrors = New Dictionary(Of String, clsValidationErrorSummary)
                    Dim messageNumber = 0
                    Dim nextSortWeight = 1

                    currentTask = "Summarize validation errors in queuedMailContainer.Value"
                    For Each queuedMailItem In queuedMailContainer.Value

                        If queuedMailItem Is Nothing Then
                            msg = "queuedMailItem is nothing for " & queuedMailContainer.Key
                            If TraceMode Then ShowTraceMessage(msg)
                            m_Logger.PostEntry(msg, logMsgType.logError, LOG_DATABASE)
                            Continue For
                        End If

                        messageNumber += 1

                        If String.IsNullOrWhiteSpace(queuedMailItem.InstrumentDatasetPath) Then
                            statusMsg = String.Format("XML File {0}: queuedMailItem.InstrumentDatasetPath is empty", messageNumber)
                        Else
                            statusMsg = String.Format("XML File {0}: {1}", messageNumber, queuedMailItem.InstrumentDatasetPath)
                            If Not instrumentFilePaths.Contains(queuedMailItem.InstrumentDatasetPath) Then
                                instrumentFilePaths.Add(queuedMailItem.InstrumentDatasetPath)
                            End If
                        End If

                        If TraceMode Then ShowTraceMessage(statusMsg)
                        m_Logger.PostEntry(statusMsg, logMsgType.logDebug, LOG_LOCAL_ONLY)

                        currentTask = "Iterate over queuedMailItem.ValidationErrors, message " & messageNumber
                        For Each validationError In queuedMailItem.ValidationErrors

                            Dim errorSummary As clsValidationErrorSummary = Nothing
                            If Not summarizedErrors.TryGetValue(validationError.IssueType, errorSummary) Then
                                errorSummary = New clsValidationErrorSummary(validationError.IssueType, nextSortWeight)
                                nextSortWeight += 1
                                summarizedErrors.Add(validationError.IssueType, errorSummary)
                            End If

                            Dim affectedItem As New udtAffectedItem
                            affectedItem.IssueDetail = validationError.IssueDetail
                            affectedItem.AdditionalInfo = validationError.AdditionalInfo

                            errorSummary.AffectedItems.Add(affectedItem)

                            If Not String.IsNullOrWhiteSpace(queuedMailItem.DatabaseErrorMsg) Then
                                If Not databaseErrorMsgs.Contains(queuedMailItem.DatabaseErrorMsg) Then
                                    databaseErrorMsgs.Add(queuedMailItem.DatabaseErrorMsg)
                                    errorSummary.DatabaseErrorMsg = queuedMailItem.DatabaseErrorMsg
                                End If
                            End If

                        Next

                        If Not subjectList.Contains(queuedMailItem.Subject) Then
                            subjectList.Add(queuedMailItem.Subject)
                        End If

                    Next

                    currentTask = "Iterate over summarizedErrors, sorted by SortWeight"

                    Dim additionalInfoList = New List(Of String)

                    For Each errorEntry In (From item In summarizedErrors Order By item.Value.SortWeight Select item)
                        Dim errorSummary = errorEntry.Value

                        Dim affectedItems = (From item In errorSummary.AffectedItems Where Not String.IsNullOrWhiteSpace(item.IssueDetail) Select item).ToList()

                        If affectedItems.Count > 0 Then
                            mailBody.AppendLine(errorEntry.Key & ": ")
                            For Each affectedItem In affectedItems

                                mailBody.AppendLine("  " & affectedItem.IssueDetail)

                                If Not String.IsNullOrWhiteSpace(affectedItem.AdditionalInfo) Then
                                    If Not String.Equals(additionalInfoList.LastOrDefault, affectedItem.AdditionalInfo) Then
                                        additionalInfoList.Add(affectedItem.AdditionalInfo)
                                    End If
                                End If

                            Next

                            For Each infoItem In additionalInfoList
                                ' Add the cached additional info items
                                mailBody.AppendLine("  " & infoItem)
                            Next

                        Else
                            mailBody.AppendLine(errorEntry.Key)
                        End If

                        mailBody.AppendLine()

                        If Not String.IsNullOrWhiteSpace(errorSummary.DatabaseErrorMsg) Then
                            mailBody.AppendLine(errorSummary.DatabaseErrorMsg)
                            mailBody.AppendLine()
                        End If
                    Next

                    If instrumentFilePaths.Count = 1 Then
                        mailBody.AppendLine("Instrument file: " & ControlChars.NewLine & "  " & instrumentFilePaths(0))
                    ElseIf instrumentFilePaths.Count > 1 Then
                        mailBody.AppendLine("Instrument files:")
                        For Each triggerFile In instrumentFilePaths
                            mailBody.AppendLine("  " & triggerFile)
                        Next
                    End If

                    currentTask = "Examine subject"
                    If subjectList.Count > 1 Then
                        ' Possibly update the subject of the e-mail
                        ' Common subjects:
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

                    mailBody.AppendLine()
                    mailBody.AppendLine("Log file location:")
                    mailBody.AppendLine("  " & GetLogFileSharePath())
                    mailBody.AppendLine()
                    mailBody.AppendLine("This message was sent from an account that is not monitored. If you have any questions, please reply to the list of recipients directly.")

                    If messageCount > 1 Then
                        ' Add the message count to the subject, e.g. 3x
                        mailToSend.Subject = String.Format("{0} ({1}x)", mailToSend.Subject, messageCount)
                    End If

                    mailToSend.Body = mailBody.ToString()

                    If MailDisabled Then
                        currentTask = "Cache the mail for preview"
                        mailContentPreview.AppendLine("E-mail that would be sent:")
                        mailContentPreview.AppendLine("To: " & recipients)
                        mailContentPreview.AppendLine("Subject: " & mailToSend.Subject)
                        mailContentPreview.AppendLine()
                        mailContentPreview.AppendLine(mailToSend.Body)
                        mailContentPreview.AppendLine()
                    Else
                        currentTask = "Send the mail"
                        Dim smtp As New SmtpClient(mailServer)
                        smtp.Send(mailToSend)

                        Thread.Sleep(100)
                    End If

                    If newLogFile Then
                        newLogFile = False
                    Else
                        mailLogger.WriteLine()
                        mailLogger.WriteLine("===============================================")
                    End If

                    mailLogger.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt"))
                    mailLogger.WriteLine()

                    mailLogger.WriteLine("To: " & recipients)
                    mailLogger.WriteLine("Subject: " & mailToSend.Subject)
                    mailLogger.WriteLine()
                    mailLogger.WriteLine(mailToSend.Body)

                Next

                currentTask = "Preview cached messages"
                If mailContentPreview.Length > 0 Then
                    ShowTraceMessage("Mail content preview" & ControlChars.NewLine &
                                     mailContentPreview.ToString())
                End If

            End Using
        Catch ex As Exception
            msg = "Error in SendQueuedMail, task " & currentTask & ": " & ex.Message
            If TraceMode Then ShowTraceMessage(msg)
            m_Logger.PostEntry(msg & "; " & GetExceptionStackTrace(ex), logMsgType.logError, LOG_DATABASE)

            Throw New Exception(msg, ex)
        End Try

    End Sub

    Private Sub FileWatcher_Changed(sender As Object, e As FileSystemEventArgs)
        m_ConfigChanged = True
        If m_DebugLevel > 3 Then
            m_Logger.PostEntry("Config file changed", logMsgType.logDebug, LOG_LOCAL_ONLY)
        End If
        m_FileWatcher.EnableRaisingEvents = False  'Turn off change detection until current change has been acted upon
    End Sub

    Private Sub OnDbErrorEvent(message As String)
        If TraceMode Then ShowTraceMessage("Database error message: " & message)
        m_Logger.PostEntry(message, logMsgType.logError, LOG_LOCAL_ONLY)
    End Sub

    Private Sub DeleteXmlFiles(folderPath As String, fileAgeDays As Integer)

        Dim workingDirectory = New DirectoryInfo(folderPath)

        ' Verify directory exists
        If Not workingDirectory.Exists Then
            ' There's a serious problem if the success/failure directory can't be found!!!

            Dim statusMsg As String = "Xml success/failure folder not found: " & folderPath
            If TraceMode Then ShowTraceMessage(statusMsg)

            m_Logger.PostEntry(statusMsg, logMsgType.logError, LOG_DATABASE)
            Return
        End If

        Dim deleteFailureCount = 0

        Try

            Dim xmlFiles As List(Of FileInfo) = workingDirectory.GetFiles("*.xml").ToList()

            For Each xmlFile In xmlFiles
                Dim filedate = xmlFile.LastWriteTimeUtc
                Dim daysDiff = DateTime.UtcNow.Subtract(filedate).Days
                If daysDiff > fileAgeDays Then
                    If PreviewMode Then
                        Console.WriteLine("Preview: delete old file: " & xmlFile.FullName)
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
            Return
        End Try

        If deleteFailureCount > 0 Then
            Dim errMsg =
                "Error deleting " & deleteFailureCount & " XML files at " & folderPath &
                " -- for a detailed list, see log file " & GetLogFileSharePath()

            If TraceMode Then ShowTraceMessage(errMsg)
            m_Logger.PostEntry(errMsg, logMsgType.logError, LOG_DATABASE)
        End If

        ' Everything must be OK if we got to here
        Return
    End Sub

    Private Sub DisableManagerLocally()

        If Not m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False") Then

            Dim statusMsg As String = "Error while disabling manager: " & m_MgrSettings.ErrMsg
            If TraceMode Then ShowTraceMessage(statusMsg)
            m_Logger.PostEntry(statusMsg, logMsgType.logError, LOG_LOCAL_ONLY)
        End If

    End Sub

    Public Shared Sub ShowTraceMessage(message As String)
        Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") & ": " & message)
    End Sub

End Class

