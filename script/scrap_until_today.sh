# !/bash/sh

usage() { echo "Usage: $0 [-s <start_date_string> (Ex: 2016-01-01)] [-m <finance_mode> (Ex: 1; 0: market_mode, 1: stock_mode)]" 1>&2; exit 1; }

while getopts ":s:e:" o; do
    case "${o}" in
        s)
            s=${OPTARG}
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

if [ -z "${s}" ]; then
    usage
fi

#echo "s = ${s}"

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
python ./finance_scrapy.py --time_duration_range ${s} ${finance_mode_attribute}

# usage() { echo "Usage: $0 [-d <date_string> (Ex: 2016-01-01)]" 1>&2; exit 1; }

# while getopts ":d:" o; do
#     case "${o}" in
#         d)
#             d=${OPTARG}
#             ;;
#         *)
#             usage
#             ;;
#     esac
# done
# shift $((OPTIND-1))

# if [ -z "${d}" ]; then
#     usage
# fi

# #echo "d = ${d}"

# cd ~/Projects/finance_scrapy_python
# python ./finance_scrapy.py -m USER_DEFINED -t ${d} --remove_old --check_result
