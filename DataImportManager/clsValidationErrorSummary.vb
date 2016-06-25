Public Class clsValidationErrorSummary

    Private ReadOnly mAffectedItems As List(Of String)
    Private ReadOnly mIssueType As String
    Private ReadOnly mSortWeight As Integer

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property AffectedItems As List(Of String)
        Get
            Return mAffectedItems
        End Get
    End Property

    Public Property DatabaseErrorMsg As String

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property IssueType As String
        Get
            Return mIssueType
        End Get
    End Property

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property SortWeight As Integer
        Get
            Return mSortWeight
        End Get
    End Property

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="issueType"></param>
    ''' <param name="sortWeight"></param>
    ''' <remarks></remarks>
    Public Sub New(issueType As String, sortWeight As Integer)
        mIssueType = issueType
        mSortWeight = sortWeight

        mAffectedItems = New List(Of String)
        DatabaseErrorMsg = String.Empty
    End Sub
End Class
