class ClientException(Exception):
    """
    general client exception
    Each subclass must define the command line exit code associated with the exception
    """
    pass

class TaskNotFoundException(ClientException):
    """
    Raised when the task directory is not found
    """
    exitcode = 4

class CachefileNotFoundException(ClientException):
    """
    Raised when the .requestcache file is not found inside the Task directory
    """
    exitcode = 5

class PSetNotFoundException(ClientException):
    """
    Raised when the pset file used specified in the configuration is not found
    """
    exitcode = 6
