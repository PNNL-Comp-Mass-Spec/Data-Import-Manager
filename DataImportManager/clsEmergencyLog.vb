
'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 04/30/2007
'
' Last modified 05/01/2007
'*********************************************************************************************************

Public Class clsEmergencyLog

    '*********************************************************************************************************
    'Clsss for loggng of problems prior to manager's full logging capability being available
    '*********************************************************************************************************

#Region "Methods"
    ''' <summary>
    ''' Writes a message to the emergency log, which is used prior to establishing normal logging
    ''' </summary>
    ''' <param name="LogFileNamePath"></param>
    ''' <param name="LogMsg"></param>
    ''' <remarks></remarks>
    Public Shared Sub WriteToLog(ByVal LogFileNamePath As String, ByVal LogMsg As String)

        Dim CurDate As String = Now.ToString("MM/dd/yyyy HH:mm:ss")
        My.Computer.FileSystem.WriteAllText(LogFileNamePath, CurDate & ControlChars.Tab & LogMsg & ControlChars.CrLf, True)

    End Sub
#End Region

End Class
