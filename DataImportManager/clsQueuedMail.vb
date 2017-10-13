
Public Class clsQueuedMail

#Region "Properties"

    Public ReadOnly Property InstrumentOperator As String

    ''' <summary>
    ''' Semi-colon separated list of e-mail addresses
    ''' </summary>
    ''' <remarks></remarks>
    Public ReadOnly Property Recipients As String

    Public ReadOnly Property Subject As String

    ''' <summary>
    ''' Tracks any database message errors
    ''' Also used to track suggested solutions
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property DatabaseErrorMsg As String

    Public ReadOnly Property ValidationErrors As List(Of clsValidationError)

    ''' <summary>
    ''' Tracks the path to the dataset on the instrument
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property InstrumentDatasetPath As String

#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="operatorName"></param>
    ''' <param name="recipientList"></param>
    ''' <param name="mailSubject"></param>
    ''' <param name="lstValidationErrors"></param>
    ''' <remarks></remarks>
    Public Sub New(operatorName As String, recipientList As String, mailSubject As String, lstValidationErrors As List(Of clsValidationError))

        InstrumentOperator = operatorName
        Recipients = recipientList
        Subject = mailSubject
        ValidationErrors = lstValidationErrors

        DatabaseErrorMsg = String.Empty
        InstrumentDatasetPath = String.Empty
    End Sub
End Class
