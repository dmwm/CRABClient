class ClientException(Exception):
    """
    general client exception
    """
    pass

class TaskNotFoundException(ClientException):
    """
    Raised when the task directory is not found
    """
    ## defining the command line exit code associated with the exception
    exitcode = 4

class CachefileNotFoundException(ClientException):
    """
    Raised when the .requestcache file is not found inside the Task directory
    """
    ## defining the command line exit code associated with the exception
    exitcode = 5
