
_UseCrab ()
{
    local cur
    COMPRELPY=()
    cur=${COMP_WORDS[COMP_CWORD]}
    sub=this_is_none

    for i in $(seq 1 ${#COMP_WORDS[*]}); do
        if [ "${COMP_WORDS[$i]#-}" == "${COMP_WORDS[$i]}" ]; then
            sub=${COMP_WORDS[$i]}
            break
        fi
    done

    prev=${COMP_WORDS[$((COMP_CWORD - 1))]}

    if [ "x$sub" == "x$cur" ]; then
        sub=""
    fi

    case "$sub" in
        "")
            case "$cur" in
            "")
                COMPREPLY=( $(compgen -W '--version --help -h --quiet --debug status tasks proceed checkwrite getlog checkusername checkdataset submit getoutput resubmit kill uploadlog remake report preparelocal createmyproxy setdatasetstatus setfilestatus' -- $cur) )
                ;;
            -*)
                COMPREPLY=( $(compgen -W '--version --help -h --quiet --debug' -- $cur) )
                ;;
            *)
                COMPREPLY=( $(compgen -W 'status tasks proceed checkwrite getlog checkusername checkdataset checkfile submit getoutput resubmit kill uploadlog remake report preparelocal createmyproxy setdatasetstatus setfilestatus' -- $cur) )
                ;;
            esac
            ;;

        "tasks")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --fromdate --days --status --proxy --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "checkwrite")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --site --lfn --checksum --command --proxy --voRole --voGroup' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "getlog")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --short --dump --xrootd --quantity --parallel --wait --outputpath --jobids --checksum --command --proxy --dir -d --voRole --voGroup --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "kill")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --jobids --killwarning --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "rmk")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --task --proxy --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "out")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --dump --xrootd --quantity --parallel --wait --outputpath --jobids --checksum --command --proxy --dir -d --voRole --voGroup --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "sub")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --wait --dryrun --skip-estimates --config -c --proxy --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "rep")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --outputdir --recovery --dbs --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "chk")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --site --lfn --checksum --command --proxy --voRole --voGroup' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "checkusername")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --proxy' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "submit")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --wait --dryrun --skip-estimates --config -c --proxy --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "getoutput")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --dump --xrootd --quantity --parallel --wait --outputpath --jobids --checksum --command --proxy --dir -d --voRole --voGroup --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "resubmit")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --wait --force --publication --jobids --sitewhitelist --siteblacklist --maxjobruntime --maxmemory --numcores --priority --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "status")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --long --json --summary --verboseErrors --sort --jobids --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "uplog")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --logpath --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "log")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --short --dump --xrootd --quantity --parallel --wait --outputpath --jobids --checksum --command --proxy --dir -d --voRole --voGroup --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "uploadlog")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --logpath --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "report")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --outputdir --recovery --dbs --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "proceed")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "st")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --long --json --summary --verboseErrors --sort --jobids --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "remake")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --task --proxy --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "output")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --dump --xrootd --quantity --parallel --wait --outputpath --jobids --checksum --command --proxy --dir -d --voRole --voGroup --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "preparelocal")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --jobid --enableStageout --destdir --proxy --dir -d --instance' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "createmyproxy")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --days' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "checkdataset")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --dataset' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "checkfile")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --lfn --instance --rucio-scope --checksum' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "setdatasetstatus")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --status --dataset' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;

        "setfilestatus")
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W '--help -h --status --dataset --files' -- $cur) )
                    ;;
                *)
                    COMPREPLY=( $(compgen -f $cur) )
            esac
            ;;


        *)
            COMPREPLY=( $(compgen -W 'status tasks proceed checkwrite getlog checkusername checkdataset checkfile submit getoutput resubmit kill uploadlog remake report preparelocal setdatasetstatus setfilestatus' -- $cur) )
            ;;
    esac

    return 0
}
complete -F _UseCrab -o filenames crab

