Imports PRISM.Logging
Imports System.Data.SqlClient
Imports System.IO
Imports DataImportManager.clsGlobal


Public Class clsDataImportTask
	Inherits clsDBTask

	Protected mPostTaskErrorMessage As String = String.Empty
	Protected mDBErrorMessage As String
    Protected ReadOnly mDMSInfoCache As DMSInfoCache

#Region "Properties"

    Public ReadOnly Property PostTaskErrorMessage As String
        Get
            If String.IsNullOrEmpty(mPostTaskErrorMessage) Then
                Return String.Empty
            Else
                Return mPostTaskErrorMessage
            End If
        End Get
    End Property

    Public ReadOnly Property DBErrorMessage As String
        Get
            If String.IsNullOrEmpty(mDBErrorMessage) Then
                Return String.Empty
            Else
                Return mDBErrorMessage
            End If
        End Get
    End Property
    
    Public Property PreviewMode As Boolean

#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="mgrParams"></param>
    ''' <param name="logger"></param>
    ''' <param name="dmsCache"></param>
    ''' <remarks></remarks>
    Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger, dmsCache As DMSInfoCache)
        MyBase.New(mgrParams, logger, dmsCache.DBConnection)
        mDMSInfoCache = dmsCache
    End Sub

#Region "parameters for calling stored procedure"
    Private mp_stored_proc As String
    Private mp_xmlContents As String
#End Region

    Public Function PostTask(ByVal triggerFile As FileInfo) As Boolean

        mPostTaskErrorMessage = String.Empty
        mDBErrorMessage = String.Empty

        Dim fileImported As Boolean

        Try
            ' Call request stored procedure
            mp_xmlContents = LoadXmlFileContentsIntoString(triggerFile, m_logger)
            If String.IsNullOrEmpty(mp_xmlContents) Then
                Return False
            End If
            fileImported = ImportDataTask()
        Catch ex As Exception
            m_logger.PostEntry("clsDatasetImportTask.PostTask(), Error running PostTask, " & ex.Message, ILogger.logMsgType.logError, True)
            Return False
        End Try

        Return fileImported

    End Function

    ''' <summary>
    ''' Posts the given XML to DMS5 using AddNewDataset
    ''' </summary>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Private Function ImportDataTask() As Boolean

        Dim sc As SqlCommand
        Dim Outcome As Boolean

        Try

            ' Initialize database error message
            mDBErrorMessage = String.Empty
            m_error_list.Clear()

            ' Prepare to call the stored procedure (typically AddNewDataset in DMS5)
            '
            mp_stored_proc = m_mgrParams.GetParam("storedprocedure")

            sc = New SqlCommand(mp_stored_proc, m_DBCn)
            sc.CommandType = CommandType.StoredProcedure
            sc.CommandTimeout = 45

            ' define parameters for command object
            '
            Dim myParm As SqlParameter
            '
            ' define parameter for stored procedure's return value
            '
            myParm = sc.Parameters.Add("@Return", SqlDbType.Int)
            myParm.Direction = ParameterDirection.ReturnValue
            '
            ' define parameters for the stored procedure's arguments
            '
            myParm = sc.Parameters.Add("@XmlDoc", SqlDbType.VarChar, 4000)
            myParm.Direction = ParameterDirection.Input
            myParm.Value = mp_xmlContents

            myParm = sc.Parameters.Add("@mode", SqlDbType.VarChar, 24)
            myParm.Direction = ParameterDirection.Input
            myParm.Value = "add"

            myParm = sc.Parameters.Add("@message", SqlDbType.VarChar, 512)
            myParm.Direction = ParameterDirection.Output

            If PreviewMode Then
                ShowTraceMessage("Preview: call stored procedure " & mp_stored_proc & " in database " & m_DBCn.Database)
                Return True
            End If

            If TraceMode Then ShowTraceMessage("Calling stored procedure " & mp_stored_proc & " in database " & m_DBCn.Database)

            ' execute the stored procedure
            '
            sc.ExecuteNonQuery()

            ' get return value
            '
            Dim ret As Integer
            ret = CInt(sc.Parameters("@Return").Value)

            If ret = 0 Then
                ' get values for output parameters
                '
                Outcome = True
            Else
                mPostTaskErrorMessage = CStr(sc.Parameters("@message").Value)
                m_logger.PostEntry("clsDataImportTask.ImportDataTask(), Problem posting dataset: " & mPostTaskErrorMessage, ILogger.logMsgType.logError, True)
                Outcome = False
            End If

        Catch ex As Exception
            m_logger.PostError("clsDataImportTask.ImportDataTask(), Error posting dataset: ", ex, True)
            mDBErrorMessage = ControlChars.NewLine & "Database Error Message:" & ex.Message
            Outcome = False
        End Try

        LogErrorEvents()

        ' Set variable for email error
        If m_error_list.Count > 0 Then
            Dim errorMsgList = String.Empty
            For Each errMsg In m_error_list
                errorMsgList = ControlChars.NewLine & errMsg & errorMsgList
            Next
            mDBErrorMessage = ControlChars.NewLine & "Database Error Message:" & errorMsgList
        End If

        Return Outcome

    End Function

End Class
