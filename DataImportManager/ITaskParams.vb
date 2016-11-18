Public Interface ITaskParams
    ' Used for job closeout
    Enum CloseOutType
        CLOSEOUT_SUCCESS = 0
        CLOSEOUT_FAILED = 1
        CLOSEOUT_NO_DATA = 10
    End Enum

End Interface
