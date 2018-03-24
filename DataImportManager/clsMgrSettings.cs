
'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 04/26/2007
'
'*********************************************************************************************************

Imports System.Xml
Imports System.Windows.Forms

#Region "Interfaces"

Public Interface IMgrParams
    Function GetParam(itemKey As String) As String

    Sub SetParam(itemKey As String, itemValue As String)
End Interface

#End Region

''' <summary>
''' Class for loading, storing and accessing manager parameters.
''' </summary>
''' <remarks>
'''	Loads initial settings from local config file, then checks to see if remainder of settings should be
'''	loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
'''	parameters database.
''' </remarks>
Public Class clsMgrSettings
    Implements IMgrParams

#Region "Module variables"
    Private m_ParamDictionary As Dictionary(Of String, String)
    Private ReadOnly m_EmerLogFile As String
    Private m_ErrMsg As String = String.Empty
    Private m_ManagerDeactivated As Boolean
#End Region

#Region "Properties"
    Public ReadOnly Property ErrMsg As String
        Get
            Return m_ErrMsg
        End Get
    End Property

    Public ReadOnly Property ManagerDeactivated As Boolean
        Get
            Return m_ManagerDeactivated
        End Get
    End Property
#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks>Logs errors to a file because logging hasn't been set up. Throws exception if a problem occurs</remarks>
    Public Sub New(emergencyLogFileNamePath As String)

        m_EmerLogFile = emergencyLogFileNamePath

        If Not LoadSettings(False) Then
            If Not m_ManagerDeactivated Then
                Throw New ApplicationException("Unable to initialize manager settings class")
            End If
        End If

    End Sub

    ''' <summary>
    ''' Loads manager settings from config file and database
    ''' </summary>
    ''' <param name="Reload">True if reloading as manager is running</param>
    ''' <returns>True if successful; False on error</returns>
    ''' <remarks></remarks>
    Public Function LoadSettings(reload As Boolean) As Boolean

        m_ErrMsg = String.Empty

        ' If reloading, clear out the existing parameter string dictionary
        If reload Then
            If m_ParamDictionary IsNot Nothing Then
                m_ParamDictionary.Clear()
            End If
            m_ParamDictionary = Nothing
        End If

        ' Get settings from config file
        m_ParamDictionary = LoadMgrSettingsFromFile()

        ' Test the settings retrieved from the config file
        If Not CheckInitialSettings() Then
            ' Error logging handled by CheckInitialSettings
            Return False
        End If

        ' Determine if manager is deactivated locally
        Dim strMgrActive As String = GetParam("MgrActive_Local")
        If String.IsNullOrEmpty(strMgrActive) Then
            m_ManagerDeactivated = True
            clsEmergencyLog.WriteToLog(m_EmerLogFile, "Manager setting MgrActive_Local not defined")
            m_ErrMsg = "Manager deactivated locally"
            Return False
        ElseIf Not CBool(strMgrActive) Then
            m_ManagerDeactivated = True
            clsEmergencyLog.WriteToLog(m_EmerLogFile, "Manager deactivated locally")
            m_ErrMsg = "Manager deactivated locally"
            Return False
        End If

        ' Get remaining settings from database
        If Not LoadMgrSettingsFromDB() Then
            ' Error logging handled by LoadMgrSettingsFromDB
            Return False
        End If

        ' If reloading, clear the "first run" flag
        If reload Then
            UpdateManagerSetting("FirstRun", "False")
        Else
            UpdateManagerSetting("FirstRun", "True")
        End If

        ' No problems found
        Return True

    End Function

    ''' <summary>
    ''' Tests initial settings retrieved from config file
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function CheckInitialSettings() As Boolean

        Dim MyMsg As String

        ' Verify manager settings dictionary exists
        If m_ParamDictionary Is Nothing Then
            MyMsg = "Manager parameter string dictionary m_ParamDictionary is null"
            clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
            Return False
        End If

        ' Verify intact config file was found
        Dim strUsingDefaults As String = GetParam("UsingDefaults")
        If String.IsNullOrEmpty(strUsingDefaults) Then
            MyMsg = "Config file problem; UsingDefaults manager setting not found"
            clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
            Return False
        End If

        Dim blnUsingDefaults As Boolean
        If Not Boolean.TryParse(strUsingDefaults, blnUsingDefaults) Then
            MyMsg = "Config file problem; UsingDefaults manager setting is not True or False"
            clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
            Return False
        End If

        If blnUsingDefaults Then
            MyMsg = "Config file problem; default settings being used (UsingDefaults=True)"
            clsEmergencyLog.WriteToLog(m_EmerLogFile, MyMsg)
            Return False
        End If

        ' No problems found
        Return True

    End Function

    ''' <summary>
    ''' Loads the initial settings from application config file
    ''' </summary>
    ''' <returns>String dictionary containing initial settings if suceessful; NOTHING on error</returns>
    ''' <remarks></remarks>
    Private Function LoadMgrSettingsFromFile() As Dictionary(Of String, String)

        ' Load initial settings into string dictionary for return
        Dim mgrSettings As New Dictionary(Of String, String)(StringComparer.OrdinalIgnoreCase)

        My.Settings.Reload()
        mgrSettings.Add("MgrCnfgDbConnectStr", My.Settings.MgrCnfgDbConnectStr)
        mgrSettings.Add("MgrActive_Local", My.Settings.MgrActive_Local.ToString)
        mgrSettings.Add("MgrName", My.Settings.MgrName)
        mgrSettings.Add("UsingDefaults", My.Settings.UsingDefaults.ToString)

        Return mgrSettings

    End Function

    ''' <summary>
    ''' Gets remaining manager config settings from config database
    ''' </summary>
    ''' <returns>True for success; False for error</returns>
    ''' <remarks></remarks>
    Private Function LoadMgrSettingsFromDB() As Boolean

        ' Requests job parameters from database. Input string specifies view to use. Performs retries if necessary.

        Dim connectionString As String = GetParam("MgrCnfgDbConnectStr")
        Dim mgrName As String = GetParam("MgrName")

        If String.IsNullOrWhiteSpace(connectionString) Then
            Dim msg = "Manager settings does not contain key MgrCnfgDbConnectStr"
            clsEmergencyLog.WriteToLog(m_EmerLogFile, msg)
            Return False
        End If

        If String.IsNullOrWhiteSpace(mgrName) Then
            Dim msg = "Manager settings does not contain key MgrName"
            clsEmergencyLog.WriteToLog(m_EmerLogFile, msg)
            Return False
        End If

        Dim sqlQuery As String = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" & mgrName & "'"

        Dim dbTools = New PRISM.clsDBTools(connectionString)
        AddHandler dbTools.ErrorEvent, AddressOf DBToolsErrorHandler

        ' Get a table holding the parameters for the manager
        Dim lstResults As List(Of List(Of String)) = Nothing

        Dim success = dbTools.GetQueryResults(sqlQuery, lstResults, "LoadMgrSettingsFromDB")

        If Not success OrElse lstResults Is Nothing Then
            Dim msg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database"
            clsEmergencyLog.WriteToLog(m_EmerLogFile, msg)
            Return False
        End If

        ' Verify at least one row returned
        If lstResults.Count < 1 Then
            ' Wrong number of rows returned
            Dim msg = "clsMgrSettings.LoadMgrSettingsFromDB; results table is empty"
            clsEmergencyLog.WriteToLog(m_EmerLogFile, msg)
            Return False
        End If

        ' Fill a string dictionary with the manager parameters that have been found

        Try
            For Each item In lstResults
                ' Add the column heading and value to the dictionary
                Dim paramName = item(0)
                Dim paramValue = item(1)
                UpdateManagerSetting(paramName, paramValue)
            Next
            Return True
        Catch ex As Exception
            Dim msg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table: " & ex.Message
            clsEmergencyLog.WriteToLog(m_EmerLogFile, msg)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Gets a parameter from the parameters string dictionary
    ''' </summary>
    ''' <param name="itemKey">Key name for item</param>
    ''' <returns>String value associated with specified key</returns>
    ''' <remarks>Returns Nothing if key isn't found</remarks>
    Public Function GetParam(itemKey As String) As String Implements IMgrParams.GetParam

        Dim strValue As String = String.Empty

        If m_ParamDictionary Is Nothing Then Return String.Empty

        If Not m_ParamDictionary.TryGetValue(itemKey, strValue) Then
            Return String.Empty
        End If

        If String.IsNullOrEmpty(strValue) Then
            Return String.Empty
        Else
            Return strValue
        End If

    End Function

    ''' <summary>
    ''' Sets a parameter in the parameters string dictionary
    ''' </summary>
    ''' <param name="itemKey">Key name for the item</param>
    ''' <param name="itemValue">Value to assign to the key</param>
    ''' <remarks></remarks>
    Public Sub SetParam(itemKey As String, itemValue As String) Implements IMgrParams.SetParam

        UpdateManagerSetting(itemKey, itemValue)

    End Sub

    ''' <summary>
    ''' Gets a collection representing all keys in the parameters string dictionary
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function GetAllKeys() As ICollection

        Return m_ParamDictionary.Keys

    End Function

    Public Sub UpdateManagerSetting(key As String, value As String)
        If m_ParamDictionary.ContainsKey(key) Then
            m_ParamDictionary(key) = value
        Else
            m_ParamDictionary.Add(key, value)
        End If
    End Sub

    ''' <summary>
    ''' Writes specfied value to an application config file.
    ''' </summary>
    ''' <param name="Key">Name for parameter (case sensitive)</param>
    ''' <param name="Value">New value for parameter</param>
    ''' <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
    ''' <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
    Public Function WriteConfigSetting(key As String, value As String) As Boolean

        m_ErrMsg = String.Empty

        'Load the config document
        Dim MyDoc As XmlDocument = LoadConfigDocument()
        If MyDoc Is Nothing Then
            'Error message has already been produced by LoadConfigDocument
            Return False
        End If

        'Retrieve the settings node
        Dim MyNode As XmlNode = MyDoc.SelectSingleNode("//applicationSettings")

        If MyNode Is Nothing Then
            m_ErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found"
            Return False
        End If

        Try
            'Select the element containing the value for the specified key containing the key
            Dim MyElement = CType(MyNode.SelectSingleNode(String.Format("//setting[@name='{0}']/value", key)), XmlElement)
            If MyElement IsNot Nothing Then
                'Set key to specified value
                MyElement.InnerText = value
            Else
                'Key was not found
                m_ErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " & key
                Return False
            End If
            MyDoc.Save(GetConfigFilePath())
            Return True
        Catch ex As Exception
            m_ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " & ex.Message
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Loads an app config file for changing parameters
    ''' </summary>
    ''' <returns>App config file as an XML document if successful; NOTHING on failure</returns>
    ''' <remarks></remarks>
    Private Function LoadConfigDocument() As XmlDocument

        Try
            Dim MyDoc = New XmlDocument
            MyDoc.Load(GetConfigFilePath)
            Return MyDoc
        Catch ex As Exception
            m_ErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " & ex.Message
            Return Nothing
        End Try

    End Function

    ''' <summary>
    ''' Specifies the full name and path for the application config file
    ''' </summary>
    ''' <returns>String containing full name and path</returns>
    ''' <remarks></remarks>
    Private Function GetConfigFilePath() As String

        Return Application.ExecutablePath & ".config"

    End Function

    Private Sub DBToolsErrorHandler(message As String, ex As Exception)
        clsEmergencyLog.WriteToLog(m_EmerLogFile, message)
    End Sub

#End Region

End Class
