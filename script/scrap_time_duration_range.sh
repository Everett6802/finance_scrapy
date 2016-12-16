# !/bash/sh

usage() { echo "Usage: $0 [-s <start_date_string> (Ex: 2016-01-01)] [-e <end_date_string> (Ex: 2016-01-01)] [-m <finance_mode> (Ex: 1; 0: market_mode, 1: stock_mode)]" 1>&2; exit 1; }

while getopts ":s:e:" o; do
    case "${o}" in
        s)
            s=${OPTARG}
            ;;
        e)
            e=${OPTARG}
            ;;
        m)
            m=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${s}" ] || [ -z "${e}" ]; then
    usage
fi

#echo "s = ${s}"
#echo "e = ${e}"

if [ -z "${m}" ]; then
    echo "Get the finance mode from market_stock_switch.conf"
else
    if [ "$m" == 0 ]; then 
        finance_mode_attribute="--market_mode"
    elif [ "$m" == 1 ]; then
        finance_mode_attribute="--stock_mode"
    else
        echo "Error !!! Unknown finance mode: $m"
        exit 1;
    fi
fi

cd ~/Projects/finance_scrapy_python
python ./finance_scrapy.py --time_duration_range ${s},${e} ${finance_mode_attribute}

# cd ~/Projects/finance_scrapy_python
# python ./finance_scrapy.py -m USER_DEFINED -t ${s},${e} --remove_old --check_result
