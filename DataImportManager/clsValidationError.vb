Public Class clsValidationError
    Public ReadOnly Property IssueType As String

    Public ReadOnly Property IssueDetail As String

    Public Property AdditionalInfo As String

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New(issueType As String, issueDetail As String)
        Me.IssueType = issueType
        Me.IssueDetail = issueDetail
        AdditionalInfo = String.Empty
    End Sub
End Class
