# !/bash/sh

usage() { echo "Usage: $0 [-s <start_date_string> (Ex: 2016-01-01)]  [-e <end_date_string> (Ex: 2016-01-01)]" 1>&2; exit 1; }

while getopts ":s:e:" o; do
    case "${o}" in
        s)
            s=${OPTARG}
            ;;
        e)
            e=${OPTARG}
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

cd ~/Projects/finance_scrapy_python
python ./finance_scrapy.py -m USER_DEFINED -t ${s},${e} --remove_old --check_result
