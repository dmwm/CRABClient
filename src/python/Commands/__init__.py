"""
Commands are stateless functions that interact with the server or local file system on
behalf of the user and return data to the calling method/object. They can print out via
their logger and it's valid for them to return no data.

Command functions are given a logger, an instance of ServerInteractions an instance of
CredentialInteractions and the commands options. They return an instance of the
CommandResult named tuple defined in this file.

This should be a library of simple functions.
"""

from collections import namedtuple

CommandResult = namedtuple('CommandResult', 'exit_code, data')

from Commands.server_info import server_info
from Commands.status import status
from Commands.submit import submit
from Commands.job_types import job_types


__all__ = ['CommandResult', 'submit', 'status', 'server_info', 'job_types']
