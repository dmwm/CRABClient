class ClientException(Exception):
    """
    general client exception
    Each subclass must define the command line exit code associated with the exception
    exitcode > 3000 does not print logfile at the end of the command
    """
    pass

class TaskNotFoundException(ClientException):
    """
    Raised when the task directory is not found
    """
    exitcode = 3004

class CachefileNotFoundException(ClientException):
    """
    Raised when the .requestcache file is not found inside the Task directory
    """
    exitcode = 3005

class PSetNotFoundException(ClientException):
    """
    Raised when the pset file used specified in the configuration is not found
    """
    exitcode = 3006

class InputFileNotFoundException(ClientException):
    """
    Raised when a file in config.JobType.inputFiles cannot be found
    """
    exitcode = 3007

class ProxyCreationException(ClientException):
    """
    Raised when a there is a problem in proxy creation. exitcode > 2000 prints log
    """
    exitcode = 8
