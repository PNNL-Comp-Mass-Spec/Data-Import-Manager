Option Strict On

Imports System.Reflection
Imports System.Collections.Generic
Imports System.IO

Module modMain
    Public Const PROGRAM_DATE As String = "June 3, 2015"

    Private mMailDisabled As Boolean
    Private mTraceMode As Boolean
    Private mPreviewMode As Boolean

    Public Function Main() As Integer
        ' Returns 0 if no error, error code if an error

        Dim intReturnCode As Integer
        Dim objParseCommandLine As New clsParseCommandLine

        mMailDisabled = False
        mTraceMode = False
        mPreviewMode = False

        Try

            ' Parse the command line options
            '
            If objParseCommandLine.ParseCommandLine Then
                SetOptionsUsingCommandLineParameters(objParseCommandLine)
            End If

            If objParseCommandLine.NeedToShowHelp Then
                ShowProgramHelp()
                intReturnCode = -1
            Else
                If mTraceMode Then ShowTraceMessage("Command line arguments parsed")

                ' Initiate automated analysis
                If mTraceMode Then ShowTraceMessage("Instantiating clsMainProcess")

                Dim oMainProcess = New clsMainProcess(mTraceMode)
                oMainProcess.MailDisabled = mMailDisabled
                oMainProcess.PreviewMode = mPreviewMode

                Try
                    If Not oMainProcess.InitMgr() Then
                        If mTraceMode Then ShowTraceMessage("InitMgr returned false")
                        Return -2
                    End If

                    If mTraceMode Then ShowTraceMessage("Manager initialized")

                Catch ex As Exception
                    ShowErrorMessage("Exception thrown by InitMgr: " & Environment.NewLine & ex.Message)
                End Try

                oMainProcess.DoImport()
                intReturnCode = 0

            End If

        Catch ex As Exception
            ShowErrorMessage("Error occurred in modMain->Main: " & Environment.NewLine & ex.Message)
            intReturnCode = -1
        End Try

        Return intReturnCode

    End Function

    Private Function GetAppPath() As String
        Return Assembly.GetExecutingAssembly().Location
    End Function

    ''' <summary>
    ''' Returns the .NET assembly version followed by the program date
    ''' </summary>
    ''' <param name="strProgramDate"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function GetAppVersion(strProgramDate As String) As String
        Return Assembly.GetExecutingAssembly().GetName().Version.ToString() & " (" & strProgramDate & ")"
    End Function

    Private Function SetOptionsUsingCommandLineParameters(objParseCommandLine As clsParseCommandLine) As Boolean
        ' Returns True if no problems; otherwise, returns false

        Dim lstValidParameters As List(Of String) = New List(Of String) From {"NoMail", "Trace", "Preview"}

        Try
            ' Make sure no invalid parameters are present
            If objParseCommandLine.InvalidParametersPresent(lstValidParameters) Then
                ShowErrorMessage("Invalid commmand line parameters",
                  (From item In objParseCommandLine.InvalidParameters(lstValidParameters) Select "/" + item).ToList())
                Return False
            Else

                ' Query objParseCommandLine to see if various parameters are present

                If objParseCommandLine.IsParameterPresent("NoMail") Then mMailDisabled = True
                If objParseCommandLine.IsParameterPresent("Trace") Then mTraceMode = True
                If objParseCommandLine.IsParameterPresent("Preview") Then mPreviewMode = True

                If mPreviewMode Then
                    mMailDisabled = True
                    mTraceMode = True
                End If

                Return True
            End If

        Catch ex As Exception
            ShowErrorMessage("Error parsing the command line parameters: " & Environment.NewLine & ex.Message)
        End Try

        Return False

    End Function

    Private Sub ShowErrorMessage(strMessage As String)
        Const strSeparator As String = "------------------------------------------------------------------------------"

        Console.WriteLine()
        Console.WriteLine(strSeparator)
        Console.WriteLine(strMessage)
        Console.WriteLine(strSeparator)
        Console.WriteLine()

        WriteToErrorStream(strMessage)
    End Sub

    Private Sub ShowErrorMessage(strTitle As String, items As IEnumerable(Of String))
        Const strSeparator As String = "------------------------------------------------------------------------------"
        Dim strMessage As String

        Console.WriteLine()
        Console.WriteLine(strSeparator)
        Console.WriteLine(strTitle)
        strMessage = strTitle & ":"

        For Each item As String In items
            Console.WriteLine("   " + item)
            strMessage &= " " & item
        Next
        Console.WriteLine(strSeparator)
        Console.WriteLine()

        WriteToErrorStream(strMessage)
    End Sub

    Private Sub ShowProgramHelp()

        Try

            Console.WriteLine("This program parses the instrument trigger files used for adding datasets to DMS. Normal operation is to run the program without any command line switches.")
            Console.WriteLine()
            Console.WriteLine("Program syntax:" & ControlChars.NewLine & Path.GetFileName(GetAppPath()) & " [/NoMail] [/Trace] [/Preview]")
            Console.WriteLine()

            Console.WriteLine("Use /NoMail to disable sending e-mail when errors are encountered")
            Console.WriteLine()

            Console.WriteLine("Use /Trace to enable trace mode, where debug messages are written to the command prompt")
            Console.WriteLine()

            Console.WriteLine("Use /Preview to enable preview mode, where we report any trigger files found, but do not " +
                              "post them to DMS and do not move them to the failure folder if there is an error. " +
                              "Using /Preview forces /NoMail and /Trace to both be enabled")
            Console.WriteLine()

            Console.WriteLine("Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)")
            Console.WriteLine()

            Console.WriteLine("Version: " & GetAppVersion(PROGRAM_DATE))
            Console.WriteLine()

            Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com")
            Console.WriteLine("Website: http://omics.pnl.gov/ or http://panomics.pnnl.gov/")
            Console.WriteLine()

            Console.WriteLine("Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License.  " & _
                  "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0")
            Console.WriteLine()

            ' Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
            System.Threading.Thread.Sleep(750)

        Catch ex As Exception
            ShowErrorMessage("Error displaying the program syntax: " & ex.Message)
        End Try

    End Sub

    Private Sub WriteToErrorStream(strErrorMessage As String)
        Try
            Using swErrorStream As StreamWriter = New StreamWriter(Console.OpenStandardError())
                swErrorStream.WriteLine(strErrorMessage)
            End Using
        Catch ex As Exception
            ' Ignore errors here
        End Try
    End Sub

    Public Sub ShowTraceMessage(strMessage As String)        
        clsMainProcess.ShowTraceMessage(strMessage)
    End Sub

End Module
