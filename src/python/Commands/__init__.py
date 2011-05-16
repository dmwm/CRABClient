"""
SubCommands __call__ method returns an instance of the
CommandResult named tuple defined in this file.
"""

from collections import namedtuple

CommandResult = namedtuple('CommandResult', 'exit_code, data')

__all__ = ['CommandResult']
