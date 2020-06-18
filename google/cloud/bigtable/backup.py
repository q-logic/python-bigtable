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

# import warnings
# from datetime import datetime

from google.cloud._helpers import _datetime_to_pb_timestamp
from google.cloud.bigtable_admin_v2.gapic import enums

# from google.cloud.bigtable_admin_v2.proto import table_pb2
# from google.cloud.bigtable_admin_v2.proto import bigtable_table_admin_pb2
from google.cloud.exceptions import NotFound

_BACKUP_NAME_RE = re.compile(
    r"^projects/(?P<project>[^/]+)/"
    r"instances/(?P<instance_id>[a-z][-a-z0-9]*)/"
    r"clusters/(?P<cluster_id>[a-z][-a-z0-9]*)/"
    r"backups/(?P<backup_id>[a-z][a-z0-9_\-]*[a-z0-9])$"
)
_DEFAULT_EXPIRE_PERIOD = 604800  # 7 days


class Backup(object):
    """Representation of a Google Cloud Bigtable Backup.

    A :class: `Backup` can be used to:

    * :meth:`create` the backup
    * :meth:`update` the backup
    * :meth:`delete` the backup

    :type backup_id: str
    :param backup_id: The ID of the backup.

    :type instance: :class:`~google.cloud.spanner_v1.instance.Instance`
    :param instance: The Instance that owns this Backup.

    :type cluster_id: str
    :param cluster_id: (Optional) The ID of the Cluster that contains this Backup.
                       Required for calling 'delete', 'exists' etc. methods.

    :type table_id: str
    :param table_id: (Optional) The ID of the Table that the Backup is for.
                     Required if the 'create' method will be called.

    :type expire_time: :class:`datetime.datetime`
    :param expire_time: (Optional) The expiration time after which the Backup
                        will be automatically deleted. Required if the `create`
                        method will be called.
    """

    def __init__(
        self, backup_id, instance, cluster_id=None, table_id=None, expire_time=None
    ):
        self.backup_id = backup_id
        self._instance = instance
        self._cluster = cluster_id
        self.table_id = table_id
        self._expire_time = expire_time

        self._parent = None
        self._source_table = None
        self._start_time = None
        self._end_time = None
        self._size_bytes = None
        self._state = None

    @property
    def name(self):
        """Backup name used in requests.

        The Backup name is of the form

            ``"projects/../instances/../clusters/../backups/{backup_id}"``

        :rtype: str
        :returns: The Backup name.

        :raises: ValueError: If the 'cluster' has not been set.
        """
        if not self._cluster:
            raise ValueError('"cluster" parameter must be set')

        return "{}/clusters/{}/backups/{}".format(
            self._instance.name, self._cluster, self.backup_id
        )

    @property
    def cluster(self):
        """The ID of the [parent] cluster used in requests.

        :rtype: str
        :returns: The ID of the cluster containing the Backup.
        """
        return self._cluster

    @cluster.setter
    def cluster(self, cluster_id):
        self._cluster = cluster_id

    @property
    def parent(self):
        """Name of the parent cluster used in requests.

        .. note::
          This property will return None if ``cluster`` is not set.

        The parent name is of the form

            ``"projects/{project}/instances/{instance_id}/clusters/{cluster}"``

        :rtype: str
        :returns: A full path to the parent cluster.
        """
        if not self._parent and self._cluster:
            self._parent = self._instance.name + "/clusters/" + self._cluster
        return self._parent

    @property
    def source_table(self):
        """The full name of the Table from which this Backup is created.

        The table name is of the form

            ``"projects/../instances/../tables/{source_table}"``

        :rtype: str
        :returns: The Table name.
        """
        if not self._source_table and self.table_id:
            self._source_table = "{}/tables/{}".format(
                self._instance.name, self.table_id
            )
        return self._source_table

    @property
    def expire_time(self):
        """Expiration time used in the creation requests.

        :rtype: :class:`datetime.datetime`
        :returns: A 'datetime' object representing the expiration time of
                  this Backup.
        """
        return self._expire_time

    @expire_time.setter
    def expire_time(self, new_expire_time):
        self._expire_time = new_expire_time

    @property
    def start_time(self):
        """The time this Backup was started.

        :rtype: :class:`datetime.datetime`
        :returns: A 'datetime' object representing the time when the creation
                  of this Backup had started.
        """
        return self._start_time

    @property
    def end_time(self):
        """The time this Backup was finished.

        :rtype: :class:`datetime.datetime`
        :returns: A 'datetime' object representing the time when the creation
                  of this Backup was finished.
        """
        return self._end_time

    @property
    def size_bytes(self):
        """The size of this Backup, in bytes.

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
        """Creates a Backup instance from a protobuf message.

        :type backup_pb: :class:`table_pb2.Backup`
        :param backup_pb: A Backup protobuf object.

        :type instance: :class:`Instance <google.cloud.bigtable.instance.Instance>`
        :param instance: The Instance that owns the Backup.

        :rtype: :class:`~google.cloud.bigtable.backup.Backup`
        :returns: The backup parsed from the protobuf response.
        :raises: ValueError: If the backup name does not match the expected
                             format or the parsed project ID does not match the
                             project ID on the Instance's client, or if the
                             parsed instance ID does not match the Instance ID.
        """
        match = _BACKUP_NAME_RE.match(backup_pb.name)
        if match is None:
            raise ValueError(
                "Backup protobuf name was not in the expected format.", backup_pb.name
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

    def create(self, cluster_id=None):
        """Creates this backup within its instance.

        :type cluster_id: str
        :param cluster_id: (Optional) The ID of the Cluster for the newly
                           created Backup.

        :rtype: :class:`~google.api_core.operation.Operation`
        :returns: :class:`~google.cloud.bigtable_admin_v2.types._OperationFuture`
                  instance, to be used to poll the status of the 'create' request
        :raises Conflict: if the Backup already exists
        :raises NotFound: if the Instance owning the Backup does not exist
        :raises BadRequest: if the `table` or `expire_time` values are invalid,
                            or `expire_time` is not set
        """
        if not self._expire_time:
            raise ValueError('"expire_time" parameter must be set')
            # TODO: Consider implementing a method that sets a default value of
            #  `expire_time`, e.g. 1 week from the creation of the Backup.
        if not self.table_id:
            raise ValueError('"table" parameter must be set')

        if cluster_id:
            self._cluster = cluster_id

        if not self._cluster:
            raise ValueError('"cluster" parameter must be set')

        backup = {
            "source_table": self.source_table,
            "expire_time": _datetime_to_pb_timestamp(self.expire_time),
        }

        api = self._instance._client.table_admin_client
        return api.create_backup(self.parent, self.backup_id, backup)

    def get(self):
        """Retrieves metadata of a pending or completed Backup.

        :returns: An instance of
                 :class:`~google.cloud.bigtable_admin_v2.types.Backup`

        :raises google.api_core.exceptions.GoogleAPICallError: If the request
                failed for any reason.
        :raises google.api_core.exceptions.RetryError: If the request failed
                due to a retryable error and retry attempts failed.
        :raises ValueError: If the parameters are invalid.
        """
        api = self._instance._client.table_admin_client
        try:
            return api.get_backup(self.name)
        except NotFound:
            return None

    def refresh(self):
        """Refreshes the stored backup properties."""
        backup = self.get()
        self._source_table = backup.source_table
        self._expire_time = backup.expire_time
        self._start_time = backup.start_time
        self._end_time = backup.end_time
        self._size_bytes = backup.size_bytes
        self._state = backup.state

    def exists(self):
        """Tests whether this Backup exists.

        :rtype: bool
        :returns: True if the Backup exists, else False.
        """
        return self.get() is not None

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
        api.update_backup(backup_update, update_mask)
        self._expire_time = new_expire_time

    def is_ready(self):
        """Tests whether this Backup is ready for use.

        :rtype: bool
        :returns: True if the Backup state is READY, otherwise False.
        """
        return self._state == enums.Backup.State.READY

    def delete(self):
        """Delete this Backup."""
        self._instance._client.table_admin_client.delete_backup(self.name)

    def restore(self, table_id):
        """Creates a new Table by restoring from this Backup. The new Table
        must be in the same Instance as the Instance containing the Backup.
        The returned Table ``long-running operation`` can be used to track the
        progress of the operation and to cancel it. The ``response`` type is
        ``Table``, if successful.

        :param table_id: The ID of the Table to create and restore to.
                         This Table must not already exist.
        :returns: An instance of
             :class:`~google.cloud.bigtable_admin_v2.types._OperationFuture`.

        :raises: google.api_core.exceptions.AlreadyExists: If the table
                 already exists.
        :raises: google.api_core.exceptions.GoogleAPICallError: If the request
                 failed for any reason.
        :raises: google.api_core.exceptions.RetryError: If the request failed
                 due to a retryable error and retry attempts failed.
        :raises: ValueError: If the parameters are invalid.
        """
        api = self._instance._client.table_admin_client
        return api.restore_table(self._instance.name, table_id, self.name)
