
from backup import Backup
from client import Client
# from datetime import datetime

PROJECT_ID = 'grass-clump-479'
INSTANCE_ID = 'bigtable-dataflowjob-test'
TABLE_ID = 'benchmark_table_10-cells_100-bytes'
CLUSTER_ID = 'bigtableio-test-c1'
BACKUP_ID = 'test-backup'

EXPIRE_TIME = 3600 * 24 * 7


client = Client(PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)
backup = Backup(BACKUP_ID, instance, CLUSTER_ID, source_table=TABLE_ID)

print(backup)

# now = datetime.now()
# print(now)
#
# expire = now.timestamp() + EXPIRE_TIME
# expire_time = datetime.fromtimestamp(expire)
# print(expire_time)

future = backup.create()

print(future)
