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

"""User friendly container for Cloud Bigtable Backup."""

import re

from google.cloud._helpers import _datetime_to_pb_timestamp
from google.cloud.bigtable_admin_v2.proto import table_pb2
from google.cloud.bigtable_admin_v2.proto import bigtable_table_admin_pb2
from google.cloud.exceptions import NotFound

_BACKUP_NAME_RE = re.compile(
    r"^projects/(?P<project>[^/]+)/"
    r"instances/(?P<instance_id>[a-z][-a-z0-9]*)/"
    r"clusters/(?P<cluster_id>[a-z][-a-z0-9]*)/"
    r"backups/(?P<backup_id>[a-z][a-z0-9_\-]*[a-z0-9])$"
)


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
	:param expire_time: (Optional) The expiration time that will be used to
	                    create the Backup. Required if the `create` method
	                    needs to be called.
	"""

	def __init__(self, backup_id, instance, table=None, expire_time=None):
		self.backup_id = backup_id
		self._instance = instance
		self._table = table
		self._expire_time = expire_time
		self._start_time = None
		self._end_time = None
		self._size_bytes = None
		self._state = None

	@property
	def name(self):
		"""Backup name used in requests.

		The Backup name is of the form

			``"projects/../instances/../backups/{backup_id}"``

		:rtype: str
		:returns: The Backup name.
		"""
		return self._instance.name + "/backups/" + self.backup_id

	@property
	def table(self):
		""" The name of the Table from which this Backup is created.

		The table name is of the form

			``"projects/../instances/../tables/{source_table}"``

		:rtype: str
		:returns: The Table name.
		"""
		return self._table

	@property
	def expire_time(self):
		""" Expiration time used in the creation requests.

		:rtype: :class:`datetime.datetime`
		:returns: A 'datetime' object representing the expiration time of
				  this Backup.
		"""
		return self._expire_time

	@property
	def start_time(self):
		""" The time this Backup was started.

		:rtype: :class:`datetime.datetime`
		:returns: A 'datetime' object representing the time when the creation
				  of this Backup was created.
		"""
		return self._start_time

	@property
	def end_time(self):
		""" The time this Backup was finished.

		:rtype: :class:`datetime.datetime`
		:returns: A 'datetime' object representing the time when the creation
				  of this Backup was finished.
		"""
		return self._end_time

	@property
	def size_bytes(self):
		""" The size of this Backup, in bytes.

		:rtype: int
		:returns: The size of this Backup, in bytes.
		"""
		return self._size_bytes

	@property
	def state(self):
		""" The current state of this Backup.

		:rtype: :class:`~google.cloud.bigtable_admin_v2.gapic.enums.Backup.State`
		:returns: The current state of this Backup.
		"""
		return self._state

	@classmethod
	def from_pb(cls, backup_pb, instance):
		""" Creates a Backup instance from a protobuf message.

		:type backup_pb: :class:`table_pb2.Backup`
		:param backup_pb: A Backup protobuf object.

		:type instance: :class:`Instance <google.cloud.bigtable.instance.Instance>`
		:param instance: The Instance that owns the Backup.

		:rtype: :class:`~google.cloud.bigtable.backup.Backup`
		:returns: The backup parsed from the protobuf response.
		:raises ValueError:
		    If the backup name does not match the expected format or the parsed
		    project ID does not match the project ID on the instance's client,
		    or if the parsed instance ID does not match the instance's ID.
		"""
		match = _BACKUP_NAME_RE.match(backup_pb.name)
		if match is None:
			raise ValueError(
				"Backup protobuf name was not in the expected format.",
				backup_pb.name
			)
		if match.group("project") != instance._client.project:
			raise ValueError(
				"Project ID of the Backup does not match the Project ID "
				"of the instance's client"
			)

		instance_id = match.group("instance_id")
		if instance_id != instance.instance_id:
			raise ValueError(
				"Instance ID of the Backup does not match the Instance ID "
				"of the instance"
			)
		backup_id = match.group("backup_id")
		return cls(backup_id, instance)

	def _metadata(self):
		return [("google-cloud-bigtable-backup", self.name)]

	def create(self):
		""" Creates this backup within its instance.

		:rtype: :class:`~google.api_core.operation.Operation`
		:returns: :class:`~google.cloud.bigtable_admin_v2.types._OperationFuture`
				  instance, to be used to poll the status of the 'create' request
		:raises Conflict: if the Backup already exists
		:raises NotFound: if the Instance owning the Backup does not exist
		:raises BadRequest: if the `table` or `expire_time` values are invalid,
							or `expire_time` is not set
		"""
		if not self._expire_time:
			raise ValueError("expire_time not set")
		if not self._table:
			raise ValueError("database not set")

		backup = {
			"table": self._table,
			"expire_time": _datetime_to_pb_timestamp(self.expire_time),
		}

		api = self._instance._client.table_admin_client
		future = api.create_backup(
			self._instance.name,
			self.backup_id,
			backup,
			metadata=self._metadata
		)
		return future

	def exists(self):
		""" Tests whether this Backup exists.

		:rtype: bool
		:returns: True if the Backup exists, else False.
		"""
		try:
			api = self._instance._client.table_admin_client
			api.get_backup(self.name, metadata=self._metadata)
		except NotFound:
			return False
		return True

	def get(self):
		""" Retrieves metadata of a pending or completed Backup.

		:return: An instance of
				 :class:`~google.cloud.bigtable_admin_v2.types.Backup`
		"""
		try:
			api = self._instance._client.table_admin_client
			backup = api.get_backup(self.name, metadata=self._metadata)
		except NotFound:
			return None
		return backup

	def update_expire_time(self, new_expire_time):
		"""Update the expire time of this Backup.

		:type new_expire_time: :class:`datetime.datetime`
		:param new_expire_time: the new expiration time timestamp
		"""
		backup_update = {
			"name": self.name,
			"expire_time": _datetime_to_pb_timestamp(new_expire_time),
		}
		update_mask = {"paths": ["expire_time"]}
		api = self._instance._client.table_admin_client
		api.update_backup(
			backup_update, update_mask, metadata=self._metadata
		)
		self._expire_time = new_expire_time

	def update(self):
		# TODO: Determine whether this wrapper is necessary
		raise NotImplementedError

	def delete(self):
		"""Delete this backup."""
		api = self._instance._client.table_admin_client
		api.delete_backup(self.name, metadata=self._metadata)

	def restore(self, table_id):
		""" Creates a new Table by restoring from this Backup. The new Table
        must be in the same Instance as the Instance containing the Backup.
        The returned Table ``long-running operation`` can be used to track the
        progress of the operation and to cancel it. The ``response`` type is
        ``Table``, if successful.

        TODO: Consider overwriting the existing Table, if necessary.

		:param table_id: The ID of the Table to create and restore to.
						 This Table must not already exist.
		:return: An instance of
		 	:class:`~google.cloud.bigtable_admin_v2.types._OperationFuture`.
		"""
		api = self._instance._client.table_admin_client
		future = api.create_backup(
			self._instance.name,
			table_id,
			self.backup_id,
			metadata=self._metadata
		)
		return future

	def optimize(self):
		raise NotImplementedError
