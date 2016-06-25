
Public Class clsQueuedMail

    Private ReadOnly mOperator As String
    Private ReadOnly mRecipients As String
    Private ReadOnly mSubject As String
    Private ReadOnly mValidationErrors As List(Of clsValidationError)

#Region "Properties"

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property InstrumentOperator As String
        Get
            Return mOperator
        End Get
    End Property

    ' ReSharper disable once ConvertToVbAutoProperty
    ''' <summary>
    ''' Semi-colon separated list of e-mail addresses
    ''' </summary>
    ''' <remarks></remarks>
    Public ReadOnly Property Recipients As String
        Get
            Return mRecipients
        End Get
    End Property

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property Subject As String
        Get
            Return mSubject
        End Get
    End Property

    ''' <summary>
    ''' Tracks any database message errors
    ''' Also used to track suggested solutions
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property DatabaseErrorMsg As String

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property ValidationErrors As List(Of clsValidationError)
        Get
            Return mValidationErrors
        End Get
    End Property

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
    ''' <param name="recipients"></param>
    ''' <param name="mailSubject"></param>
    ''' <param name="validationErrors"></param>
    ''' <remarks></remarks>
    Public Sub New(
      operatorName As String,
      recipients As String,
      mailSubject As String,
      validationErrors As List(Of clsValidationError))

        mOperator = operatorName
        mRecipients = recipients
        mSubject = mailSubject
        mValidationErrors = validationErrors

        DatabaseErrorMsg = String.Empty
        InstrumentDatasetPath = String.Empty
    End Sub
End Class
