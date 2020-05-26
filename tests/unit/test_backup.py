# Copyright 2020 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import datetime
import mock
import unittest

from google.cloud._helpers import UTC



class TestBackup(unittest.TestCase):
	BACKUP_ID = 'backup-id'
	INSTANCE_ID = 'instance-id'
	INSTANCE_NAME = 'projects/instances/' + INSTANCE_ID
	TABLE_ID = 'table-id'

	@staticmethod
	def _get_target_class():
		from google.cloud.bigtable.backup import Backup

		return Backup

	def _make_one(self, *args, **kwargs):
		return self._get_target_class()(*args, **kwargs)

	def test_constructor_defaults(self):
		instance = _Instance(self.INSTANCE_NAME)
		backup = self._make_one(self.BACKUP_ID, instance)

		self.assertEqual(backup.backup_id, self.BACKUP_ID)
		self.assertIs(backup._instance, instance)
		self.assertIsNone(backup._table)
		self.assertIsNone(backup._expire_time)
		self.assertIsNone(backup._create_time)

	def test_constructor_non_defaults(self):
		instance = _Instance(self.INSTANCE_NAME)
		expire_time = datetime.datetime.utcnow().replace(tzinfo=UTC)

		backup = self._make_one(
			self.BACKUP_ID,
			instance,
			table=self.TABLE_ID,
			expire_time=expire_time
		)

		self.assertEqual(backup.backup_id, self.BACKUP_ID)
		self.assertIs(backup._instance, instance)
		self.assertEqual(backup._table, self.TABLE_ID)
		self.assertEqual(backup._expire_time, expire_time)
		self.assertIsNone(backup._create_time)

	def test_from_pb_project_mismatch(self):
		pass

	def test_from_pb_instance_mismatch(self):
		pass

	def test_from_pb_bad_name(self):
		pass

	def test_from_pb_success(self):
		pass

	def test_property_name(self):
		pass

	def test_property_table(self):
		pass

	def test_property_expire_time(self):
		pass

	def test_property_create_time(self):
		pass

	def test_property_size(self):
		pass

	def test_property_state(self):
		pass

	def test_create_grpc_error(self):
		pass

	def test_create_already_exists(self):
		pass

	def test_create_instance_not_found(self):
		pass

	def test_create_table_not_set(self):
		pass

	def test_create_expire_time_not_set(self):
		pass

	def test_create_success(self):
		pass

	def test_exists_grpc_error(self):
		pass

	def test_exists_not_found(self):
		pass

	def test_exists_success(self):
		pass

	def test_delete_grpc_error(self):
		pass

	def test_delete_not_found(self):
		pass

	def test_delete_success(self):
		pass

	def test_update_expire_time_grpc_error(self):
		pass

	def test_update_expire_time_not_found(self):
		pass

	def test_update_expire_time_success(self):
		pass

	def test_is_ready(self):
		pass



class _Instance(object):
	def __init__(self, name, client=None):
		self.name = name
		self.instance_id = name.rsplit("/", 1)[1]
		self._client = client
