Imports System.Net.Mail

Public Class clsQueuedMail

    Private ReadOnly mOperator As String
    Private ReadOnly mRecipients As String

    Private ReadOnly mMailMessage As MailMessage

#Region "Properties"

    ' ReSharper disable once ConvertToVbAutoProperty
    ''' <summary>
    ''' Message object
    ''' </summary>
    ''' <remarks></remarks>
    Public ReadOnly Property Mail As MailMessage
        Get
            Return mMailMessage
        End Get
    End Property

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
            Return mMailMessage.Subject
        End Get
    End Property

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property Body As String
        Get
            Return mMailMessage.Body
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

#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="operatorName"></param>
    ''' <param name="recipients"></param>
    ''' <param name="message"></param>
    ''' <remarks></remarks>
    Public Sub New(operatorName As String, recipients As String, message As MailMessage)
        mOperator = operatorName
        mRecipients = recipients
        mMailMessage = message

        DatabaseErrorMsg = String.Empty
    End Sub
End Class
