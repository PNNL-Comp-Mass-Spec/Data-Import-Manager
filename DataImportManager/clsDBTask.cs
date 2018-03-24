Imports System.Data.SqlClient

Public MustInherit Class clsDBTask

#Region "Member Variables"

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
    ''' <param name="dbConnection">Database connection object (connection should already be open)</param>
    ''' <remarks></remarks>
    Public Sub New(mgrParams As IMgrParams, dbConnection As SqlConnection)
        m_mgrParams = mgrParams
        m_DBCn = dbConnection
    End Sub

End Class

