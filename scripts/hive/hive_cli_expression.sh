hive -e "$1"
exit_code=$?
echo "Statement: $1"
exit $exit_code
