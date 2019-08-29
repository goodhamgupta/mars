hive -f $1
exit_code=$?
echo "$1 execution successful!"
exit $exit_code
