Imports System.Threading
Imports System.Runtime.CompilerServices

Module modExtensionMethods

    ''' <summary>
    ''' This class is used by xmlFilesToImport.Shuffle()
    ''' in DoDataImportTask
    ''' </summary>
    Public NotInheritable Class ThreadSafeRandom

        Private Sub New()
        End Sub

        <ThreadStatic> Private Shared mRandGenerator As New Random

        Public Shared ReadOnly Property ThisThreadsRandom As Random
            Get
                Return If(mRandGenerator, (InlineAssignHelper(mRandGenerator, New Random(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId))))
            End Get
        End Property

        Private Shared Function InlineAssignHelper(Of T)(ByRef target As T, value As T) As T
            target = value
            Return value
        End Function
    End Class

    <Extension>
    Public Sub Shuffle(Of T)(list As IList(Of T))
        Dim n As Integer = list.Count
        While n > 1
            n -= 1
            Dim k As Integer = ThreadSafeRandom.ThisThreadsRandom.Next(n + 1)

            ' Swap items
            Dim value As T = list(k)
            list(k) = list(n)
            list(n) = value
        End While
    End Sub

End Module
