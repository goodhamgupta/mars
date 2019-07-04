aws s3 cp s3://datum-mars/auth/milkyway/read_replica_password.txt /home/hadoop/

chmod 400 /home/hadoop/read_replica_password.txt

sqoop import \
  --connect jdbc:mysql://$1:$2/$3 \
  --username $4 \
  --password-file /home/hadoop/read_replica_password.txt \
  --table $5 \
  --external-table-dir $6 \
  --num-mappers 1 \
  --as-textfile \
  --hive-import \
  --hive-overwrite \
  --hive-delims-replacement ' '
