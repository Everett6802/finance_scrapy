# !/bash/sh

usage() { echo "Usage: $0 [-s <source_type_index> (Ex: 6; range: 0~8)]" 1>&2; exit 1; }

while getopts ":s:" o; do
    case "${o}" in
        s)
            s=${OPTARG}
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

if (( 0 <= $s && $s < 8 )); then 
	finance_mode_attribute="--market_mode"
elif (( 8 <= $s && $s < 9 )); then
	finance_mode_attribute="--stock_mode"
else
	echo "Error !!! Unknown type index $s"
	exit 1;
fi

#echo "s = ${s}"

cd ~/Projects/finance_scrapy_python
python ./finance_scrapy.py --debug_source ${s} ${finance_mode_attribute}
