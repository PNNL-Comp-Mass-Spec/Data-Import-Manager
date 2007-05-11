Imports PRISM.Logging

Public Interface IDataImportResources

    ReadOnly Property Message() As String

    Sub Setup(ByVal mgrParams As IMgrParams, ByVal taskParams As ITaskParams, ByVal logger As ILogger)

    Function GetResources() As ITaskParams.CloseOutType

End Interface
