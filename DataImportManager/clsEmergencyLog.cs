'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 04/30/2007
'
'*********************************************************************************************************
Imports System.IO

''' <summary>
''' Class for logging of problems prior to manager's full logging capability being available
''' </summary>
Public Class clsEmergencyLog

    ''' <summary>
    ''' Writes a message to the emergency log, which is used prior to establishing normal logging
    ''' </summary>
    ''' <param name="logFilePath"></param>
    ''' <param name="message"></param>
    ''' <remarks></remarks>
    Public Shared Sub WriteToLog(logFilePath As String, message As String)

        Dim timeStamp As String = DateTime.Now().ToString("MM/dd/yyyy HH:mm:ss")
        Using writer = New StreamWriter(New FileStream(logFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
            writer.WriteLine(timeStamp & ControlChars.Tab & message)
        End Using

    End Sub

End Class
