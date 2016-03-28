# !/bash/sh

usage() { echo "Usage: $0 [-s <data_source_index> (Ex: 6; range: 0~10)]" 1>&2; exit 1; }

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

#echo "s = ${s}"

cd ~/Projects/finance_scrapy_python
python ./finance_scrapy.py --do_debug ${s}
