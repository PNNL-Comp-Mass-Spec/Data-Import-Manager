Public Class clsValidationErrorSummary

    Public Structure udtAffectedItem
        Public IssueDetail As String
        Public AdditionalInfo As String
    End Structure

    ' ReSharper disable once CollectionNeverUpdated.Global
    ' This property is used clsMainProcess
    Public ReadOnly Property AffectedItems As List(Of udtAffectedItem)

    Public Property DatabaseErrorMsg As String

    Public ReadOnly Property IssueType As String

    Public ReadOnly Property SortWeight As Integer

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="issueType"></param>
    ''' <param name="sortWeight"></param>
    ''' <remarks></remarks>
    Public Sub New(issueType As String, sortWeight As Integer)
        Me.IssueType = issueType
        Me.SortWeight = sortWeight

        AffectedItems = New List(Of udtAffectedItem)
        DatabaseErrorMsg = String.Empty
    End Sub

End Class
