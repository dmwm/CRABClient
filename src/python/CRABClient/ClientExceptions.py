FEEDBACKMAIL = 'hn-cms-computing-tools@cern.ch'

class ClientException(Exception):
    """
    general client exception
    Each subclass must define the command line exit code associated with the exception
    exitcode > 3000 does not print logfile at the end of the command.
    """
    pass


class TaskNotFoundException(ClientException):
    """
    Raised when the task directory is not found.
    """
    exitcode = 3004

class CachefileNotFoundException(ClientException):
    """
    Raised when the .requestcache file is not found inside the Task directory.
    """
    exitcode = 3005

class ConfigException(ClientException):
    """
    Raised when there are issues with the config cache.
    """
    exitcode = 3006

class InputFileNotFoundException(ClientException):
    """
    Raised when a file in config.JobType.inputFiles cannot be found.
    """
    exitcode = 3007

class ConfigurationException(ClientException):
    """
    Raised when there is an issue with configuration/command line parameters.
    """
    exitcode = 3008

class MissingOptionException(ConfigurationException):
    """
    Raised when a mandatory option is not found in the command line.
    """
    exitcode = 3008
    missingOption = None

class RESTCommunicationException(ClientException):
    """
    Raised when the REST does not answer the 200 HTTP code.
    """
    exitcode = 3009

class ProxyCreationException(ClientException):
    """
    Raised when there is a problem in proxy creation. exitcode > 2000 prints log
    """
    exitcode = 3010

class EnvironmentException(ClientException):
    """
    Raised when there is a problem in the environment where the client is executed.
    E.g.: if the X509_CERT_DIR variable is not set we raise this exception.
    """
    exitcode = 3011

class UsernameException(ClientException):
    """
    Raised when there is a problem with the username (e.g. when retrieving it from SiteDB).
    """
    exitcode = 3012

class ProxyException(ClientException):
    """
    Raised when there is a problem with the proxy (e.g. proxy file not found).
    """
    exitcode = 3013

class UnknownOptionException(ClientException):
    """
    Raised when an unknown option is specified in the command line.
    """
    exitcode = 3014

class PanDaException(ClientException):
    """
    Specific errors coming from interaction with PanDa
    """
    exitcode = 3100
