Imports PRISM.Logging
Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports DataImportManager.clsGlobal
Imports System.Xml
Imports System.IO
Imports DataImportManager.MgrSettings


Public Class clsDataImportTask
    Inherits clsDBTask
    Public mp_db_err_msg As String
    ' constructor
    Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger)
        MyBase.New(mgrParams, logger)
    End Sub

#Region "parameters for calling stored procedure"
    Private mp_stored_proc As String
    Private mp_xmlContents As String
#End Region

    Public Function PostTask(ByVal xmlFile As String) As Boolean

        m_connection_str = m_mgrParams.GetParam("ConnectionString")
        Dim m_fileImported As Boolean
        m_fileImported = False

        Try
            OpenConnection()
        Catch Err As System.Exception
            m_logger.PostEntry("clsDatasetImportTask.PostTask(), error opening connection, " & Err.Message, ILogger.logMsgType.logError, True)
            Return False
        End Try

        Try
            ' call request stored procedure
            mp_xmlContents = RetrieveXmlFileContents(xmlFile)
            If mp_xmlContents = "" Then
                Return False
            End If
            m_fileImported = ImportDataTask(mp_xmlContents)
        Catch Err As System.Exception
            m_logger.PostEntry("clsDatasetImportTask.PostTask(), Error running PostTask, " & Err.Message, ILogger.logMsgType.logError, True)
            Return False
        End Try

        Try
            CloseConnection()
        Catch Err As System.Exception
            m_logger.PostEntry("clsDatasetImportTask.PostTask(), Error closing connection, " & Err.Message, ILogger.logMsgType.logError, True)
            Return False
        End Try

        Return m_fileImported

    End Function

    Private Function RetrieveXmlFileContents(ByVal xmlFile As String) As String
        Dim xmlFileContents As String
        Try
            xmlFileContents = ""
            'Read the contents of the xml file into a string which will be passed into a stored procedure.
            If Not File.Exists(xmlFile) Then
                m_logger.PostEntry("clsDataImportTask.RetrieveXmlFileContents(), File: " & xmlFile & " does not exist.", ILogger.logMsgType.logError, True)
            End If
            Dim sr As StreamReader = File.OpenText(xmlFile)
            Dim input As String
            input = sr.ReadLine()
            xmlFileContents = input
            While Not input Is Nothing
                input = sr.ReadLine()
                xmlFileContents = xmlFileContents + Chr(13) + Chr(10) + input
            End While
            sr.Close()
            Return xmlFileContents
        Catch Err As System.Exception
            m_logger.PostEntry("clsDataImportTask.RetrieveXmlFileContents(), Error reading xml file, " & Err.Message, ILogger.logMsgType.logError, True)
            Return ""
        End Try

    End Function

    Public Sub CloseTask(ByVal closeOut As ITaskParams.CloseOutType, ByVal resultsFolderName As String, ByVal comment As String)
        If (closeOut = ITaskParams.CloseOutType.CLOSEOUT_SUCCESS) Or (closeOut = ITaskParams.CloseOutType.CLOSEOUT_NO_DATA) Then
            FailCount = 0
        Else
            FailCount += 1
        End If
    End Sub

    Private Function GetCompletionCode(ByVal closeOut As ITaskParams.CloseOutType) As Integer
        Dim code As Integer = 1    '  0->success, 1->failure, anything else ->no intermediate files
        Select Case closeOut
            Case ITaskParams.CloseOutType.CLOSEOUT_SUCCESS
                code = 0
            Case ITaskParams.CloseOutType.CLOSEOUT_FAILED
                code = 1
            Case ITaskParams.CloseOutType.CLOSEOUT_NO_DATA
                code = 10
        End Select
        GetCompletionCode = code
    End Function

    '------[for DB access]-----------------------------------------------------------

    Private Function ImportDataTask(ByVal mp_xmlContents As String) As Boolean

        Dim sc As SqlCommand
        Dim Outcome As Boolean = False

        Try
            m_error_list.Clear()
            ' create the command object
            '
            mp_stored_proc = m_mgrParams.GetParam("storedprocedure")

            sc = New SqlCommand(mp_stored_proc, m_DBCn)
            sc.CommandType = CommandType.StoredProcedure

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

            myParm = sc.Parameters.Add("@message", SqlDbType.VarChar, 512)
            myParm.Direction = ParameterDirection.Output

            ' execute the stored procedure
            '
            sc.ExecuteNonQuery()

            ' get return value
            '
            '        Dim ret As Object
            Dim ret As Integer
            ret = CInt(sc.Parameters("@Return").Value)

            If ret = 0 Then
                ' get values for output parameters
                '
                Outcome = True
            Else
                m_logger.PostEntry("Problem posting dataset: " _
                  & CStr(sc.Parameters("@message").Value), ILogger.logMsgType.logError, True)
                Outcome = False
            End If

        Catch ex As System.Exception
            m_logger.PostError("Error posting dataset: ", ex, True)
            Outcome = False
        End Try

        LogErrorEvents()
        'Set variable for email error
        If m_error_list.Count > 0 Then
            Dim s As String
            Dim tmp_s As String
            mp_db_err_msg = ""
            tmp_s = ""
            For Each s In m_error_list
                tmp_s = Chr(13) & Chr(10) & s & tmp_s
            Next
            mp_db_err_msg = Chr(13) & Chr(10) & "Database Error Message:" & tmp_s
        End If

        Return Outcome

    End Function

End Class
