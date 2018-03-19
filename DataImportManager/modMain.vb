Option Strict On

Imports System.Reflection
Imports System.IO
Imports PRISM
Imports PRISM.Logging

Module modMain
    Public Const PROGRAM_DATE As String = "March 19, 2018"

    Private mMailDisabled As Boolean
    Private mTraceMode As Boolean
    Private mPreviewMode As Boolean
    Private mIgnoreInstrumentSourceErrors As Boolean

    ''' <summary>
    ''' Entry method
    ''' </summary>
    ''' <returns>0 if no error, error code if an error</returns>
    Public Function Main() As Integer

        Dim commandLineParser As New clsParseCommandLine()

        mMailDisabled = False
        mTraceMode = False
        mPreviewMode = False
        mIgnoreInstrumentSourceErrors = False

        Try

            Dim validArgs As Boolean

            ' Parse the command line options
            '
            If commandLineParser.ParseCommandLine Then
                validArgs = SetOptionsUsingCommandLineParameters(commandLineParser)
            Else
                If (commandLineParser.NoParameters) Then
                    validArgs = True
                Else
                    If (commandLineParser.NeedToShowHelp) Then
                        ShowProgramHelp()
                    Else
                        ConsoleMsgUtils.ShowWarning("Error parsing the command line arguments")
                        clsParseCommandLine.PauseAtConsole(750)
                    End If
                    Return -1
                End If
            End If

            If commandLineParser.NeedToShowHelp OrElse Not validArgs Then
                ShowProgramHelp()
                Return -1
            End If

            If mTraceMode Then ShowTraceMessage("Command line arguments parsed")

                ' Initiate automated analysis
                If mTraceMode Then ShowTraceMessage("Instantiating clsMainProcess")

                Dim oMainProcess = New clsMainProcess(mTraceMode)
                oMainProcess.MailDisabled = mMailDisabled
            oMainProcess.PreviewMode = mPreviewMode
            oMainProcess.IgnoreInstrumentSourceErrors = mIgnoreInstrumentSourceErrors

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

            LogTools.FlushPendingMessages()
            Return 0

        Catch ex As Exception
            ShowErrorMessage("Error occurred in modMain->Main: " & Environment.NewLine & ex.Message)
            LogTools.FlushPendingMessages()
            Return -1
        End Try

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

    Private Function SetOptionsUsingCommandLineParameters(commandLineParser As clsParseCommandLine) As Boolean
        ' Returns True if no problems; otherwise, returns false

        Dim validParameters = New List(Of String) From {"NoMail", "Trace", "Preview", "ISE"}

        Try
            ' Make sure no invalid parameters are present
            If commandLineParser.InvalidParametersPresent(validParameters) Then
                ShowErrorMessage("Invalid commmand line parameters",
                  (From item In commandLineParser.InvalidParameters(validParameters) Select "/" + item).ToList())
                Return False
            Else

                ' Query commandLineParser to see if various parameters are present

                If commandLineParser.IsParameterPresent("NoMail") Then mMailDisabled = True
                If commandLineParser.IsParameterPresent("Trace") Then mTraceMode = True
                If commandLineParser.IsParameterPresent("Preview") Then mPreviewMode = True
                If commandLineParser.IsParameterPresent("ISE") Then mIgnoreInstrumentSourceErrors = True

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

    Private Sub ShowErrorMessage(message As String)
        ConsoleMsgUtils.ShowError(message)
    End Sub

    Private Sub ShowErrorMessage(title As String, errorMessages As IEnumerable(Of String))
        ConsoleMsgUtils.ShowErrors(title, errorMessages)
    End Sub

    Private Sub ShowProgramHelp()

        Try

            Console.WriteLine("This program parses the instrument trigger files used for adding datasets to DMS. Normal operation is to run the program without any command line switches.")
            Console.WriteLine()
            Console.WriteLine("Program syntax:" & ControlChars.NewLine & Path.GetFileName(GetAppPath()) & " [/NoMail] [/Trace] [/Preview] [/ISE]")
            Console.WriteLine()

            Console.WriteLine("Use /NoMail to disable sending e-mail when errors are encountered")
            Console.WriteLine()

            Console.WriteLine("Use /Trace to enable trace mode, where debug messages are written to the command prompt")
            Console.WriteLine()

            Console.WriteLine("Use /Preview to enable preview mode, where we report any trigger files found, but do not " +
                              "post them to DMS and do not move them to the failure folder if there is an error. " +
                              "Using /Preview forces /NoMail and /Trace to both be enabled")
            Console.WriteLine()
            Console.WriteLine("Use /ISE to ignore instrument source check errors (e.g. cannot access bionet)")
            Console.WriteLine()

            Console.WriteLine("Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)")
            Console.WriteLine()

            Console.WriteLine("Version: " & GetAppVersion(PROGRAM_DATE))
            Console.WriteLine()

            Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov")
            Console.WriteLine("Website: httsp://omics.pnl.gov/ or https://panomics.pnnl.gov/")
            Console.WriteLine()

            Console.WriteLine("Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License.  " & _
                  "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0")
            Console.WriteLine()

            ' Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
            clsParseCommandLine.PauseAtConsole(750)

        Catch ex As Exception
            ShowErrorMessage("Error displaying the program syntax: " & ex.Message)
        End Try

    End Sub

    Private Sub ShowTraceMessage(message As String)
        clsMainProcess.ShowTraceMessage(message)
    End Sub

End Module
