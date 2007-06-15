
'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 04/26/2007
'
' Last modified 05/01/2007
'*********************************************************************************************************
Imports PRISM.Logging
Imports System.data.SqlClient
Imports System.IO
Imports System.Collections.Specialized

Namespace MgrSettings
#Region "Interfaces"
    Public Interface IMgrParams
        Function GetParam(ByVal ItemKey As String) As String
        Sub SetParam(ByVal ItemKey As String, ByVal ItemValue As String)
    End Interface
#End Region


    Public Class clsMgrSettings

        Implements IMgrParams

        '*********************************************************************************************************
        'Class for loading, storing and accessing manager parameters.
        '	Loads initial settings from local config file, then checks to see if remainder of settings should be
        '		loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
        '		parameters database.
        '*********************************************************************************************************

#Region "Module variables"
        Private m_ParamDictionary As StringDictionary
        Private m_EmerLogFile As String
#End Region

#Region "Methods"
        ''' <summary>
        ''' Constructor
        ''' </summary>
        ''' <remarks>Logs errors to a file because logging hasn't been set up. Throws exception if a problem occurs</remarks>
        Public Sub New(ByVal EmergencyLogFileNamePath As String)

            m_EmerLogFile = EmergencyLogFileNamePath

            If Not LoadSettings(False) Then
                Throw New ApplicationException("Unable to initialize manager settings class")
            End If

        End Sub

        ''' <summary>
        ''' Loads manager settings from config file and database
        ''' </summary>
        ''' <param name="Reload">True if reloading as manager is running</param>
        ''' <returns>True if successful; False on error</returns>
        ''' <remarks></remarks>
        Public Function LoadSettings(ByVal Reload As Boolean) As Boolean

            'If reloading, clear out the existing parameter string dictionary
            If Reload Then
                If m_ParamDictionary IsNot Nothing Then
                    m_ParamDictionary.Clear()
                End If
                m_ParamDictionary = Nothing
            End If

            'Get settings from config file
            m_ParamDictionary = LoadMgrSettingsFromFile()

            'Test the settings retrieved from the config file
            If Not CheckInitialSettings(m_ParamDictionary) Then
                'Error logging handled by CheckInitialSettings
                Return False
            End If

            'Determine if manager is deactivated locally
            If Not Reload Then
                'We don't want to perform this test if reloading. It will be performed by the calling program
                If Not CBool(m_ParamDictionary("mgractive_local")) Then
                    clsEmergencyLog.WriteToLog(m_EmerLogFile, "Manager deactivated locally")
                    Return False
                End If
            End If

            'Get remaining settings from database
            If Not LoadMgrSettingsFromDB(m_ParamDictionary) Then
                'Error logging handled by LoadMgrSettingsFromDB
                Return False
            End If

            'No problems found
            Return True

        End Function

        ''' <summary>
        ''' Tests initial settings retrieved from config file
        ''' </summary>
        ''' <param name="InpDict"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function CheckInitialSettings(ByRef InpDict As StringDictionary) As Boolean

            Dim MyMsg As String

            'Verify manager settings dictionary exists
            If m_ParamDictionary Is Nothing Then
                'Error reporting is in LoadMgrSettingsFromFile, so none is required here
                Return False
            End If

            'Verify a connection string for the manager config database was found
            If m_ParamDictionary("mgrcnfgdbconnectstr") = "(none)" Then
                MyMsg = "clsMgrSettings.CheckInitialSettings; Invalid manager config db connection string: " & m_ParamDictionary("mgrcnfgdbconnectstr")
                clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
                Return False
            End If

            'Verify a manager name was found
            If m_ParamDictionary("mgrname") = "(none)" Then
                MyMsg = "clsMgrSettings.CheckInitialSettings; Invalid manager name: " & m_ParamDictionary("mgrname")
                clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
                Return False
            End If

            'No problems found
            Return True

        End Function

        ''' <summary>
        ''' Loads the initial settings from application config file
        ''' </summary>
        ''' <returns>String dictionary containing initial settings if suceessful; NOTHING on error</returns>
        ''' <remarks></remarks>
        Private Function LoadMgrSettingsFromFile() As StringDictionary

            'Load initial settings into string dictionary for return
            Dim RetDict As New StringDictionary

            Try
                My.Settings.Reload()
                RetDict.Add("mgrcnfgdbconnectstr", My.Settings.mgrcnfgdbconnectstr)
                RetDict.Add("mgractive_local", My.Settings.mgractive_local.ToString)
                RetDict.Add("mgrname", My.Settings.mgrname)
            Catch ex As Exception
                Dim MyMsg As String = "clsMgrSettings.LoadMgrSettingsFromFile; Exception reading config file: " & ex.Message
                clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
                RetDict = Nothing
            End Try

            Return RetDict

        End Function

        ''' <summary>
        ''' Gets remaining manager config settings from config database
        ''' </summary>
        ''' <param name="MgrSettingsDict">String dictionary containing parameters that have been loaded so far</param>
        ''' <returns>True for success; False for error</returns>
        ''' <remarks></remarks>
        Private Function LoadMgrSettingsFromDB(ByRef MgrSettingsDict As StringDictionary) As Boolean

            'Requests job parameters from database. Input string specifies view to use. Performs retries if necessary.

            Dim RetryCount As Short = 3
            Dim MyMsg As String

            Dim SqlStr As String = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" & _
              m_ParamDictionary("mgrname") & "'"

            'Get a table containing data for job
            Dim Dt As DataTable


            'Get a datatable holding the parameters for one job
            While RetryCount > 0
                Try
                    Using Cn As SqlConnection = New SqlConnection(MgrSettingsDict("mgrcnfgdbconnectstr"))
                        Using Da As SqlDataAdapter = New SqlDataAdapter(SqlStr, Cn)
                            Using Ds As DataSet = New DataSet
                                Da.Fill(Ds)
                                Dt = Ds.Tables(0)
                            End Using  'Ds
                        End Using  'Da
                    End Using  'Cn
                    Exit While
                Catch ex As Exception
                    RetryCount -= 1S
                    MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " & ex.Message
                    MyMsg &= ", RetryCount = " & RetryCount.ToString
                    clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
                    System.Threading.Thread.Sleep(1000)             'Delay for 1 second before trying again
                Finally
                    Dt.Dispose()
                End Try
            End While

            'If loop exited due to errors, return false
            If RetryCount < 1 Then
                MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database"
                clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
                Return False
            End If

            'Verify at least one row returned
            If Dt.Rows.Count < 1 Then
                'Wrong number of rows returned
                MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Invalid row count retrieving manager settings: RowCount = "
                MyMsg &= Dt.Rows.Count.ToString
                clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
                Return False
            End If

            'Fill a string dictionary with the manager parameters that have been found
            Dim CurRow As DataRow
            Try
                For Each CurRow In Dt.Rows
                    'Add the column heading and value to the dictionary
                    m_ParamDictionary.Add(DbCStr(CurRow(Dt.Columns("ParameterName"))), DbCStr(CurRow(Dt.Columns("ParameterValue"))))
                Next
                Return True
            Catch ex As Exception
                MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table: " & ex.Message
                clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
                Return False
            Finally
                Dt.Dispose()
            End Try

        End Function

        Protected Function DbCStr(ByVal InpObj As Object) As String

            'If input object is DbNull, returns "", otherwise returns String representation of object
            If InpObj Is DBNull.Value Then
                Return ""
            Else
                Return CStr(InpObj)
            End If

        End Function

        ''' <summary>
        ''' Gets a parameter from the parameters string dictionary
        ''' </summary>
        ''' <param name="ItemKey">Key name for item</param>
        ''' <returns>String value associated with specified key</returns>
        ''' <remarks>Returns Nothing if key isn't found</remarks>
        Public Function GetParam(ByVal ItemKey As String) As String Implements IMgrParams.GetParam

            Return m_ParamDictionary.Item(ItemKey)

        End Function

        ''' <summary>
        ''' Sets a parameter in the parameters string dictionary
        ''' </summary>
        ''' <param name="ItemKey">Key name for the item</param>
        ''' <param name="ItemValue">Value to assign to the key</param>
        ''' <remarks></remarks>
        Public Sub SetParam(ByVal ItemKey As String, ByVal ItemValue As String) Implements IMgrParams.SetParam

            m_ParamDictionary.Item(ItemKey) = ItemValue

        End Sub

        ''' <summary>
        ''' Gets a collection representing all keys in the parameters string dictionary
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetAllKeys() As ICollection

            Return m_ParamDictionary.Keys

        End Function
#End Region

    End Class

End Namespace
