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
	PROJECT_ID = 'project-id'
	INSTANCE_ID = 'instance-id'
	INSTANCE_NAME = 'projects/instances/' + INSTANCE_ID
	TABLE_ID = 'table-id'
	TABLE_NAME = INSTANCE_NAME + '/tables/' + TABLE_ID
	BACKUP_ID = 'backup-id'
	BACKUP_NAME = INSTANCE_NAME + "/backups/" + BACKUP_ID

	@staticmethod
	def _get_target_class():
		from google.cloud.bigtable.backup import Backup

		return Backup

	def _make_one(self, *args, **kwargs):
		return self._get_target_class()(*args, **kwargs)

	def _make_timestamp(self):
		return datetime.datetime.utcnow().replace(tzinfo=UTC)

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
		expire_time = self._make_timestamp()

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
		from google.cloud.bigtable_admin_v2.proto import table_pb2

		alt_project_id = 'alt-project-id'
		client = _Client(project=alt_project_id)
		instance = _Instance(self.INSTANCE_NAME, client)
		backup_pb = table_pb2.Backup(name=self.BACKUP_NAME)
		klasse = self._get_target_class()

		with self.assertRaises(ValueError):
			klasse.from_pb(backup_pb, instance)

	def test_from_pb_instance_mismatch(self):
		from google.cloud.bigtable_admin_v2.proto import table_pb2

		alt_instance = "/projects/%s/instances/alt-instance" % self.PROJECT_ID
		client = _Client()
		instance = _Instance(alt_instance, client)
		backup_pb = table_pb2.Backup(name=self.BACKUP_NAME)
		klasse = self._get_target_class()

		with self.assertRaises(ValueError):
			klasse.from_pb(backup_pb, instance)

	def test_from_pb_bad_name(self):
		from google.cloud.bigtable_admin_v2.proto import table_pb2

		client = _Client()
		instance = _Instance(self.INSTANCE_NAME, client)
		backup_pb = table_pb2.Backup(name="invalid_name")
		klasse = self._get_target_class()

		with self.assertRaises(ValueError):
			klasse.from_pb(backup_pb, instance)

	def test_from_pb_success(self):
		from google.cloud.bigtable_admin_v2.proto import table_pb2

		client = _Client()
		instance = _Instance(self.INSTANCE_NAME, client)
		backup_pb = table_pb2.Backup(name=self.BACKUP_NAME)
		klasse = self._get_target_class()

		backup = klasse.from_pb(backup_pb, instance)

		self.assertTrue(isinstance(backup, klasse))
		self.assertEqual(backup._instance, instance)
		self.assertEqual(backup.backup_id, self.BACKUP_ID)
		self.assertEqual(backup._table, "")
		self.assertIsNone(backup._expire_time)
		self.assertIsNone(backup._create_time)

	def test_property_name(self):
		instance = _Instance(self.INSTANCE_NAME)
		backup = self._make_one(self.BACKUP_ID, instance)
		expected_name = self.BACKUP_NAME
		self.assertEqual(backup.name, expected_name)

	def test_property_table(self):
		instance = _Instance(self.INSTANCE_NAME)
		backup = self._make_one(self.BACKUP_ID, instance)
		expected = backup._table = self.TABLE_NAME
		self.assertEqual(backup.table, expected)

	def test_property_expire_time(self):
		instance = _Instance(self.INSTANCE_NAME)
		backup = self._make_one(self.BACKUP_ID, instance)
		expected = backup._expire_time = self._make_timestamp()
		self.assertEqual(backup.expire_time, expected)

	def test_property_start_time(self):
		instance = _Instance(self.INSTANCE_NAME)
		backup = self._make_one(self.BACKUP_ID, instance)
		expected = backup._start_time = self._make_timestamp()
		self.assertEqual(backup.start_time, expected)

	def test_property_end_time(self):
		instance = _Instance(self.INSTANCE_NAME)
		backup = self._make_one(self.BACKUP_ID, instance)
		expected = backup._end_time = self._make_timestamp()
		self.assertEqual(backup.end_time, expected)

	def test_property_size(self):
		instance = _Instance(self.INSTANCE_NAME)
		backup = self._make_one(self.BACKUP_ID, instance)
		expected = backup._size_bytes = 10
		self.assertEqual(backup.size_bytes, expected)

	def test_property_state(self):
		from google.cloud.bigtable_admin_v2.gapic import enums

		instance = _Instance(self.INSTANCE_NAME)
		backup = self._make_one(self.BACKUP_ID, instance)
		expected = backup._state = enums.Backup.State.READY
		self.assertEqual(backup.state, expected)

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


class _Client(object):
	def __init__(self, project=TestBackup.PROJECT_ID):
		self.project = project
		self.project_name = "projects/" + self.project


class _Instance(object):
	def __init__(self, name, client=None):
		self.name = name
		self.instance_id = name.rsplit("/", 1)[1]
		self._client = client
