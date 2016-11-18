Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports PRISM.Logging

Public MustInherit Class clsDBTask

#Region "Member Variables"

    ' access to the logger
    Protected ReadOnly m_logger As ILogger

    ' access to mgr parameters
    Protected ReadOnly m_mgrParams As IMgrParams

    ' DB access
    Protected ReadOnly m_DBCn As SqlConnection

#End Region

#Region "Auto-properties"
    Public Property TraceMode As Boolean
#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="mgrParams"></param>
    ''' <param name="logger"></param>
    ''' <param name="dbConnection">Database connection object (connection should already be open)</param>
    ''' <remarks></remarks>
    Public Sub New(mgrParams As IMgrParams, logger As ILogger, dbConnection As SqlConnection)
        m_mgrParams = mgrParams
        m_logger = logger
        m_DBCn = dbConnection
    End Sub

End Class

