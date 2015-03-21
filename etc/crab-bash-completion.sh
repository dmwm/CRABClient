# with help of the following unix-chainsaw command line:
# crab help|sed -n '/^Valid/,/^To/p'|awk '/^ / {print $1}'|while read cmd; do echo $cmd\); echo "'$(crab $cmd -h|sed -ne '/^ *-/ s/.*\(--\w*\).*/\1/p')'"; done
_UseCrab ()
{
	local cur

	COMPREPLY=()
	# echo ${COMP_WORDS[1]}
	cur=${COMP_WORDS[COMP_CWORD]}
	sub=${COMP_WORDS[1]}

	case "$sub" in
		checkusername)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --proxy --voRole --voGroup' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		checkwrite)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --site --lfn --proxy --voRole --voGroup' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		getlog)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --quantity --parallel --wait --outputpath --dump --xrootd --jobids --proxy --dir --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		getoutput)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --quantity --parallel --wait --outputpath --dump --xrootd --jobids --proxy --dir --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		kill)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --jobids --proxy --dir --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		purge)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --schedd --cache --proxy --dir --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		remake)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --cmptask --proxy --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		report)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --outputdir --dbs --proxy --dir --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		resubmit)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --blacklist --whitelist --memory --cores --priority --wall --jobids --proxy --dir --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		status)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --long --json --summary --idle --sort --proxy --dir --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		submit)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --config --wait --proxy --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		tasks)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --fromdate --days --proxy --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		uploadlog)
		case "$cur" in
			-*)
			COMPREPLY=( $(compgen -W '--help --logpath --dir --proxy --voRole --voGroup --instance' -- $cur) )
			;;
			*)
			COMPREPLY=( $(compgen -f ${cur}) )
			;;
		esac
		;;
		*)
		case "$cur" in
			-*)
			COMPREPLY=( $( compgen -W '-h --help -q --quiet -d --debug --version' -- $cur ) )
			;;
			*)
			COMPREPLY=( $( compgen -W ' \
					checkusername \
					checkwrite \
					getlog \
					getoutput \
					help \
					kill \
					proceed \
					purge \
					remake \
					report \
					resubmit \
					status \
					submit \
					tasks \
					uploadlog \
			' -- $cur ) )
			;;
		esac
		;;
	esac

	return 0
}

complete -F _UseCrab -o filenames crab
