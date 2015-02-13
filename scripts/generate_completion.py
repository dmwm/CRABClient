import imp
import optparse

from CRABClient.ClientMapping import commands_configuration

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

    case "$sub" in
        "")
            if [ -z "$cur" ]; then
                COMPREPLY=( $(compgen -W '{topoptions} {topcommands}' -- $cur) )
            else
                COMPREPLY=( $(compgen -W '{topoptions}' -- $cur) )
            fi
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

commands = {}
options = []

for opt in client.parser.option_list:
    options.append(opt.get_opt_string())
    options += opt._short_opts

for k, v in client.sub_commands.items():
    class DummyCmd(v):
        def __init__(self):
            self.parser = optparse.OptionParser()
            self.logger = DummyLogger()
            self.cmdconf = commands_configuration.get(v.name)

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

    commands[cmd.name] = template_cmd.format(
            cmd=cmd.name,
            cmdflags=' '.join(flags),
            cmdoptions=' '.join(opts))

    # print cmd.name
    # print cmd.parser.option_list
    # print dir(cmd.parser.option_list[0])
    # print cmd.parser.option_list[0].nargs
    # print cmd
    # print cmd.parser
    # print dir(cmd.parser)
    # print v.name
    # cmd = v(logger, ['-h'])
    # print dir(cmd)

print template.format(
        topcommands=' '.join(commands.keys()),
        topoptions=' '.join(options),
        commands=''.join(commands.values()))
