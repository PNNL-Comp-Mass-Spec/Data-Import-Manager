Public Class clsValidationError

    Private ReadOnly mIssueType As String
    Private ReadOnly mIssueDetail As String

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property IssueType As String
        Get
            Return mIssueType
        End Get
    End Property

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property IssueDetail As String
        Get
            Return mIssueDetail
        End Get
    End Property

    Public Property AdditionalInfo As String

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New(issueType As String, issueDetail As String)
        mIssueType = issueType
        mIssueDetail = issueDetail
        AdditionalInfo = String.Empty
    End Sub
End Class
