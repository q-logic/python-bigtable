# Copyright 2015 Google LLC
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


from google.cloud._helpers import _datetime_to_pb_timestamp
from google.cloud.exceptions import NotFound


class Backup(object):
	"""Representation of a Google Cloud Bigtable Backup.

	A :class`Backup` can be used to:

	* :meth:`create` the backup
	* :meth:`update` the backup
	* :meth:`delete` the backup

	:type backup_id: str
	:param backup_id: The ID of the backup.

	:type instance: :class:`~google.cloud.spanner_v1.instance.Instance`
	:param instance: The Instance that owns the Backup.

	:type table: str
	:param table: (Optional) The ID of the Table that the Backup is for.
				  Required if the 'create' method needs to be called.

	:type expire_time: :class:`datetime.datetime`
	:param expire_time: (Optional) The expire time that will be used to
	                    create the Backup. Required if the create method
	                    needs to be called.
	"""

	def __init__(self, backup_id, instance, table=None, expire_time=None):
		self.backup_id = backup_id
		self._name = instance.name + "/backups/" + backup_id
		self._instance = instance
		self._api = instance._client.table_admin_client
		self._table = table
		self._expire_time = expire_time
		self._create_time = None
		self._metadata = [("google-cloud-bigtable-backup", self._name)]
		# self._size_bytes = None
		# self._state = None
		# self._referencing_table = None

	@property
	def name(self):
		"""Backup name used in requests.

		The backup name is of the form

			``"projects/../instances/../backups/{backup_id}"``

		:rtype: str
		:returns: The backup name.
		"""
		return self._name

	@property
	def table(self):
		""" The name of the Table used in requests.

		The table name is of the form

			``"projects/../instances/../backups/{backup_id}"``

		:rtype: str
		:returns: The table name.
		"""
		return self._table

	@property
	def expire_time(self):
		"""Expire time used in creation requests.

		:rtype: :class:`datetime.datetime`
		:returns: A 'datetime' object representing the expiration time of
				  this backup.
		"""
		return self._expire_time

	@property
	def create_time(self):
		"""Create time of this backup.

		:rtype: :class:`datetime.datetime`
		:returns: A 'datetime' object representing the time when this backup
				  was created.
		"""
		return self._create_time

	def create(self):
		""" Creates this backup within its instance.

		:rtype: :class:`~google.api_core.operation.Operation`
		:returns: :class:`~google.cloud.bigtable_admin_v2.types._OperationFuture`
				  instance, to be used to poll the status of the 'create' request
		:raises Conflict: if the backup already exists
		:raises NotFound: if the instance owning the backup does not exist
		:raises BadRequest: if the table or expire_time values are invalid
							or expire_time is not set
		"""
		if not self._expire_time:
			raise ValueError("expire_time not set")
		if not self._table:
			raise ValueError("database not set")

		backup = {
			"table": self._table,
			"expire_time": _datetime_to_pb_timestamp(self.expire_time),
		}

		future = self._api.create_backup(
			self._instance.name,
			self.backup_id,
			backup,
			metadata=self._metadata
		)
		return future

	def exists(self):
		"""Test whether this backup exists.

		:rtype: bool
		:returns: True if the backup exists, else False.
		"""
		try:
			self._api.get_backup(self._name, metadata=self._metadata)
		except NotFound:
			return False
		return True

	def get(self):
		""" Gets metadata on a pending or completed Cloud Bigtable Backup.

		:return: An instance of
				 :class:`~google.cloud.bigtable_admin_v2.types.Backup`
		"""
		try:
			backup = self._api.get_backup(self._name, metadata=self._metadata)
		except NotFound:
			return None
		return backup

	def update_expire_time(self, new_expire_time):
		"""Update the expire time of this backup.

		:type new_expire_time: :class:`datetime.datetime`
		:param new_expire_time: the new expire time timestamp
		"""
		backup_update = {
			"name": self._name,
			"expire_time": _datetime_to_pb_timestamp(new_expire_time),
		}
		update_mask = {"paths": ["expire_time"]}
		self._api.update_backup(
			backup_update, update_mask, metadata=self._metadata
		)
		self._expire_time = new_expire_time

	def update(self):
		# TODO: Determine whether this wrapper is necessary
		raise NotImplementedError

	def delete(self):
		"""Delete this backup."""
		self._api.delete_backup(self._name, metadata=self._metadata)

	def restore(self, table_id):
		""" Creates a new table by restoring from this backup. The new table
        must be in the same instance as the instance containing the backup. The
        returned table ``long-running operation`` can be used to track the
        progress of the operation, and to cancel it. The ``response`` type is
        ``Table``, if successful.

        TODO: Consider overwriting the existing table, if necessary.

		:param table_id: The ID of the table to create and restore to.
			This table must not already exist.
		:return: An instance of
		 	:class:`~google.cloud.bigtable_admin_v2.types._OperationFuture`.
		"""
		future = self._api.create_backup(
			self._instance.name,
			table_id,
			self.backup_id,
			metadata=self._metadata
		)
		return future

	def optimize(self):
		raise NotImplementedError
