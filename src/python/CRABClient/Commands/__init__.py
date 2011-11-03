"""
SubCommands __call__ method returns an instance of the
CommandResult named tuple defined in this file.
"""

from collections import namedtuple

CommandResult = namedtuple('CommandResult', 'exit_code, data')

def mergeResults( results = None ):
    if results is not None and len(results) > 0:
        msg = []
        exitcode = 0
        for res in results:
            exitcode = exitcode or res[0]
            msg.append(res[1])
        return CommandResult(exitcode, '\n'.join(msg))
    else:
        return results

__all__ = ['CommandResult', 'mergeResults']
