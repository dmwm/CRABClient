class ClientException(Exception):
    """
    general client exception
    """
    pass

class TaskNotFoundException(ClientException):
    ## defining the command line exit code associated with the exception
    exitcode = 4
