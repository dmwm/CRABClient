"""

This script is used to generate the file etc/crab-bash-completion.sh
automatically from crab client code, without the need to manually edit it.
The current crab client build process uses etc/crab-bash-completion.sh
directly.

When you change the interface of crab client, for example adding a new command
or adding a new parameter to an existing command, you should run this script 
with:

> cd CRABClient
> python3 scripts/generate_completions.py

This script will generate a new version of etc/crab-bash-completion.sh
that will be used by the crab client build process.

Known limitations:

- sorting of the suggestions can be broken despite the use of "-o nosort".
  For example when using "set completion-ignore-case on" on bash 4.4.20, 
  which is the version installed on lxplus8. 
  see https://unix.stackexchange.com/a/567937

"""

import importlib.util
import sys
import argparse
import logging

from CRABClient.CRABOptParser import CRABCmdOptParser
from CRABClient.ClientMapping import commandsConfiguration

logging.basicConfig(level=logging.INFO)

import argparse

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
complete -F _UseCrab -o filenames -o nosort crab
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

def main():

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-o", "--output-file",
      help="output completion file", type=str, 
      default="etc/crab-bash-completion.sh" )
    p_args = parser.parse_args()

    logging.info(p_args.output_file)

    # python "imp" is deprecated, migrated to "importlib" with the help of
    # https://stackoverflow.com/a/41595552
    spec = importlib.util.spec_from_file_location("crab", "bin/crab.py")
    crab = importlib.util.module_from_spec(spec)
    sys.modules["crab"] = crab
    spec.loader.exec_module(crab)

    client = crab.CRABClient()
    logger = DummyLogger()

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


    # sort the output of "crab <tab> <tab>"
    # the higher the number the earlier a crab command is shown
    weights = {
        "status": 1000,
        "tasks": 200,
        "submit": 110,
        "proceed": 100,
    }
    # current sorting as of 2024-05-14. Do we still want to keep this?
    #checkwrite getlog checkusername checkdataset checkfile 
    #submit getoutput resubmit kill uploadlog 
    #remake report preparelocal createmyproxy setdatasetstatus setfilestatus

    longnames_w = [(name, weights[name] if name in weights else 0) for name in longnames]
    longnames_w = sorted(longnames_w, key=lambda x: x[1], reverse=True)
    longnames = [l_w[0] for l_w in longnames_w]
    logging.info(longnames)

    with open(p_args.output_file, "w") as f_:
        f_.write(template.format(
            topcommands=' '.join(longnames),
            topoptions=' '.join(options),
            commands=''.join(commands.values())))

if __name__ == "__main__":
    main()
