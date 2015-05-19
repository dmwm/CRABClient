import imp
import optparse

from CRABClient.CRABOptParser import CRABCmdOptParser
from CRABClient.ClientMapping import commandsConfiguration

template = """
_UseCrab ()
{{
    local cur
    COMPRELPY=()
    cur=${{COMP_WORDS[COMP_CWORD]}}
    sub=this_is_none

    for i in $(seq 1 ${{#COMP_WORDS[*]}}); do
        if [ "${{COMP_WORDS[$i]#-}}" == "${{COMP_WORDS[$i]}}" ]; then
            sub=${{COMP_WORDS[$i]}}
            break
        fi
    done

    prev=${{COMP_WORDS[$((COMP_CWORD - 1))]}}

    if [ "x$sub" == "x$cur" ]; then
        sub=""
    fi

    case "$sub" in
        "")
            case "$cur" in
            "")
                COMPREPLY=( $(compgen -W '{topoptions} {topcommands}' -- $cur) )
                ;;
            -*)
                COMPREPLY=( $(compgen -W '{topoptions}' -- $cur) )
                ;;
            *)
                COMPREPLY=( $(compgen -W '{topcommands}' -- $cur) )
                ;;
            esac
            ;;
{commands}
        *)
            COMPREPLY=( $(compgen -W '{topcommands}' -- $cur) )
            ;;
    esac

    return 0
}}
complete -F _UseCrab -o filenames crab
"""

template_cmd = """
        "{cmd}")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '{cmdflags} {cmdoptions}' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;
"""

class DummyLogger(object):
    def debug(self, *args, **kwargs):
        pass
    @property
    def logfile(self):
        return ''

crab = imp.load_source('crab', 'bin/crab')

client = crab.CRABClient()
logger = DummyLogger()
# print template
# sys.exit(0)

longnames = []
commands = {}
options = []

for opt in client.parser.option_list:
    options.append(opt.get_opt_string())
    options += opt._short_opts

for k, v in client.subCommands.items():
    class DummyCmd(v):
        def __init__(self):
            self.parser = CRABCmdOptParser(v.name, '', False)
            self.logger = DummyLogger()
            self.cmdconf = commandsConfiguration.get(v.name)

    cmd = DummyCmd()
    cmd.setSuperOptions()

    flags = []
    opts = []

    for opt in cmd.parser.option_list:
        args = opt.nargs if opt.nargs is not None else 0
        names = [opt.get_opt_string()] + opt._short_opts

        if args == 0:
            flags += names
        else:
            opts += names

    longnames.append(cmd.name)
    for c in [cmd.name] + cmd.shortnames:
        commands[c] = template_cmd.format(
                cmd=c,
                cmdflags=' '.join(flags),
                cmdoptions=' '.join(opts))

print template.format(
        topcommands=' '.join(longnames),
        topoptions=' '.join(options),
        commands=''.join(commands.values()))
