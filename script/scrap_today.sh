# !/bash/sh

usage() { echo "Usage: $0 [-m <finance_mode> (Ex: 1; 0: market_mode, 1: stock_mode)]" 1>&2; exit 1; }

while getopts ":m:" o; do
    case "${o}" in
        m)
            m=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

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
python ./finance_scrapy.py --time_today ${finance_mode_attribute}

# cd ~/Projects/finance_scrapy_python
# python ./finance_scrapy.py -m TODAY --remove_old --check_result --run_daily
