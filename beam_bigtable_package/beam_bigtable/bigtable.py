# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Apache Beam Bigtable connector

This module implements writing to BigTable tables.
The default mode is to set row data to write to BigTable tables.
The syntax supported is described here:
https://cloud.google.com/bigtable/docs/quickstart-cbt

BigTable connector can be used as main outputs. A main output
(common case) is expected to be massive and will be split into
manageable chunks and processed in parallel. In the example below
we created a list of rows then passed to the GeneratedDirectRows
DoFn to set the Cells and then we call the BigTableWriteFn to insert
those generated rows in the table.
"""

from __future__ import absolute_import
from __future__ import division

from random import shuffle

from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import PTransform
from apache_beam.io.iobase import BoundedSource
from apache_beam.io.iobase import SourceBundle
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.transforms.display import DisplayDataItem

try:
  from google.cloud.bigtable.batcher import FLUSH_COUNT, MAX_ROW_BYTES
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.row_set import RowSet
  from google.cloud.bigtable.row_set import RowRange
except ImportError:
  Client = None
  FLUSH_COUNT = 1000
  MAX_ROW_BYTES = 100000
  RowSet = object


__all__ = ['BigtableSource', 'BigtableWrite']


class BigtableSource(BoundedSource):
  def __init__(self, project_id, instance_id, table_id, filter_=None):
    """ Constructor of the Read connector of Bigtable

    Args:
      project_id: [string] GCP Project of to write the Rows
      instance_id: [string] GCP Instance to write the Rows
      table_id: [string] GCP Table to write the `DirectRows`
      filter_: [RowFilter] Filter to apply to cells in a row.
    """
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'filter_': filter_}
    self.sample_row_keys = None
    self.table = None
    self.row_count = Metrics.counter(self.__class__.__name__, 'Source Row Counter')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = options
    self.table = None
    self.sample_row_keys = None
    self.row_count = Metrics.counter(self.__class__.__name__, 'Source Row Counter')

  def _get_table(self):
    if self.table is None:
      self.table = Client(project=self.beam_options['project_id'])\
                      .instance(self.beam_options['instance_id'])\
                      .table(self.beam_options['table_id'])
    return self.table

  def get_sample_row_keys(self):
    """ Get a sample of row keys in the table.

    The returned row keys will delimit contiguous sections of the table of
    approximately equal size, which can be used to break up the data for
    distributed tasks like mapreduces.
    :returns: A cancel-able iterator. Can be consumed by calling ``next()``
    			  or by casting to a :class:`list` and can be cancelled by
    			  calling ``cancel()``.
    """
    if self.sample_row_keys is None:
      self.sample_row_keys = list(self._get_table().sample_row_keys())
    return self.sample_row_keys

  def get_range_tracker(self, start_position=b'', stop_position=b''):
    if stop_position == b'':
      return LexicographicKeyRangeTracker(start_position)
    else:
      return LexicographicKeyRangeTracker(start_position, stop_position)

  def estimate_size(self):
    return list(self.get_sample_row_keys())[-1].offset_bytes

  def split(self, desired_bundle_size=None, start_position=None, stop_position=None):
    """ Splits the source into a set of bundles, using the row_set if it is set.

    *** At this point, only splitting an entire table into samples based on the sample row keys is supported ***

    :param desired_bundle_size: the desired size (in bytes) of the bundles returned.
    :param start_position: if specified, the position must be used as the starting position of the first bundle.
    :param stop_position: if specified, the position must be used as the ending position of the last bundle.
    Returns:
    	an iterator of objects of type 'SourceBundle' that gives information about the generated bundles.
    """

    if desired_bundle_size is not None or start_position is not None or stop_position is not None:
      raise NotImplementedError

    # TODO: Use the desired bundle size to split accordingly
    # TODO: Allow users to provide their own row sets

    sample_row_keys = list(self.get_sample_row_keys())
    bundles = []
    if len(sample_row_keys) > 0 and sample_row_keys[0] != b'':
        bundles.append(SourceBundle(sample_row_keys[0].offset_bytes, self, b'', sample_row_keys[0].row_key))
    for i in range(1, len(sample_row_keys)):
      pos_start = sample_row_keys[i - 1].offset_bytes
      pos_stop = sample_row_keys[i].offset_bytes
      bundles.append(SourceBundle(pos_stop - pos_start, self,
                                  sample_row_keys[i - 1].row_key,
                                  sample_row_keys[i].row_key))

    # Shuffle is needed to allow reading from different locations of the table for better efficiency
    shuffle(bundles)
    return bundles

  def read(self, range_tracker):
    for row in self._get_table().read_rows(start_key=range_tracker.start_position(),
                                           end_key=range_tracker.stop_position(),
                                           filter_=self.beam_options['filter_']):
      if range_tracker.try_claim(row.row_key):
        self.row_count.inc()
        yield row
      else:
        # TODO: Modify the client ot be able to cancel read_row request
        break

  def display_data(self):
    ret = {'projectId': DisplayDataItem(self.beam_options['project_id'],
                                        label='Bigtable Project Id',
                                        key='projectId'),
           'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                         label='Bigtable Instance Id',
                                         key='instanceId'),
           'tableId': DisplayDataItem(self.beam_options['table_id'],
                                      label='Bigtable Table Id',
                                      key='tableId')}
    return ret

  def to_runner_api_parameter(self, unused_context):
    pass


class _BigtableWriteFn(DoFn):
  def __init__(self, project_id, instance_id, table_id, flush_count=FLUSH_COUNT, max_row_bytes=MAX_ROW_BYTES):
    """ Constructor of the Write connector of Bigtable

		Args:
			project_id: [string] GCP Project of to write the Rows
			instance_id: [string] GCP Instance to write the Rows
			table_id: [string] GCP Table to write the `DirectRows`
		"""
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'flush_count': flush_count,
                         'max_row_bytes': max_row_bytes}
    self.table = None
    self.batcher = None
    self.row_counter = Metrics.counter(self.__class__, 'Rows Written')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = options
    self.table = None
    self.batcher = None
    self.row_counter = Metrics.counter(self.__class__, 'Rows Written')

  def start_bundle(self):
    if self.table is None:
      self.table = Client(project=self.beam_options['project_id']) \
        .instance(self.beam_options['instance_id']) \
        .table(self.beam_options['table_id'])
    self.batcher = self.table.mutations_batcher(self.beam_options['flush_count'],
                                                self.beam_options['max_row_bytes'])

  def process(self, element, *args, **kwargs):
    self.row_counter.inc()
    self.batcher.mutate(element)

  def finish_bundle(self):
    self.batcher.flush()
    self.batcher = None

  def display_data(self):
    return {'projectId': DisplayDataItem(self.beam_options['project_id'],
                                         label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                          label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(self.beam_options['table_id'],
                                       label='Bigtable Table Id')
           }

  def to_runner_api_parameter(self, unused_context):
    pass


class BigtableWrite(PTransform):
  """ A transform to write to the Bigtable Table.

  A PTransform that write a list of `DirectRow` into the Bigtable Table
  """
  def __init__(self, project_id, instance_id, table_id, flush_count=FLUSH_COUNT, max_row_bytes=MAX_ROW_BYTES):
    """ The PTransform to access the Bigtable Write connector
    Args:
      project_id: [string] GCP Project of to write the Rows
      instance_id: [string] GCP Instance to write the Rows
      table_id: [string] GCP Table to write the `DirectRows`
    """
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'flush_count': flush_count,
                         'max_row_bytes': max_row_bytes}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | ParDo(_BigtableWriteFn(beam_options['project_id'],
                                     beam_options['instance_id'],
                                     beam_options['table_id'],
                                     beam_options['flush_count'],
                                     beam_options['max_row_bytes'])))
