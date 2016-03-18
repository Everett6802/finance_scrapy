# !/bash/sh

usage() { echo "Usage: $0 [-d <date_string> (Ex: 2016-01-01)]" 1>&2; exit 1; }

while getopts ":d:" o; do
    case "${o}" in
        d)
            d=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${d}" ]; then
    usage
fi

#echo "d = ${d}"

cd ~/Projects/finance_scrapy_python
python ./finance_scrapy.py -m USER_DEFINED -t ${d},${d} --remove_old --check_result
