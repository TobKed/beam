/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A writer that manages multiple {@link RecordWriter}s to write to multiple tables and partitions.
 * Assigns one {@link DestinationState} per windowed destination. A {@link DestinationState} assigns
 * one writer per partition in table destination. If the Iceberg {@link Table} is un-partitioned,
 * the data is written normally using one {@link RecordWriter} (i.e. the {@link DestinationState}
 * has one writer). At any given moment, the number of open data writers should be less than or
 * equal to the number of total partitions (across all windowed destinations).
 *
 * <p>A {@link DestinationState} maintains its writers in a {@link Cache}. If a {@link RecordWriter}
 * is inactive for 1 minute, the {@link DestinationState} will automatically close it to free up
 * resources. Calling {@link #close()} on this {@link RecordWriterManager} will do the following for
 * each {@link DestinationState}:
 *
 * <ol>
 *   <li>Close all underlying {@link RecordWriter}s
 *   <li>Collect all {@link DataFile}s
 *   <li>Create a new {@link ManifestFile} referencing these {@link DataFile}s
 * </ol>
 *
 * <p>After closing, the resulting {@link ManifestFile}s can be retrieved using {@link
 * #getManifestFiles()}.
 */
class RecordWriterManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RecordWriterManager.class);

  /**
   * Represents the state of one Iceberg table destination. Creates one {@link RecordWriter} per
   * partition and manages them in a {@link Cache}.
   *
   * <p>On closing, each writer's output {@link DataFile} is collected.
   */
  class DestinationState {
    private final IcebergDestination icebergDestination;
    private final PartitionSpec spec;
    private final org.apache.iceberg.Schema schema;
    private final PartitionKey partitionKey;
    private final String tableLocation;
    private final FileIO fileIO;
    private final String stateToken = UUID.randomUUID().toString();
    private final List<DataFile> dataFiles = Lists.newArrayList();
    @VisibleForTesting final Cache<PartitionKey, RecordWriter> writers;
    @VisibleForTesting final Map<PartitionKey, Integer> writerCounts = Maps.newHashMap();

    DestinationState(IcebergDestination icebergDestination, Table table) {
      this.icebergDestination = icebergDestination;
      this.schema = table.schema();
      this.spec = table.spec();
      this.partitionKey = new PartitionKey(spec, schema);
      this.tableLocation = table.location();
      this.fileIO = table.io();

      // build a cache of RecordWriters.
      // writers will expire after 1 min of idle time.
      // when a writer expires, its data file is collected.
      this.writers =
          CacheBuilder.newBuilder()
              .expireAfterAccess(1, TimeUnit.MINUTES)
              .removalListener(
                  (RemovalNotification<PartitionKey, RecordWriter> removal) -> {
                    final PartitionKey pk = Preconditions.checkStateNotNull(removal.getKey());
                    final RecordWriter recordWriter =
                        Preconditions.checkStateNotNull(removal.getValue());
                    try {
                      recordWriter.close();
                    } catch (IOException e) {
                      throw new RuntimeException(
                          String.format(
                              "Encountered an error when closing data writer for table '%s', partition %s",
                              icebergDestination.getTableIdentifier(), pk),
                          e);
                    }
                    openWriters--;
                    dataFiles.add(recordWriter.getDataFile());
                  })
              .build();
    }

    /**
     * Computes the partition key for this Iceberg {@link Record} and writes it using the
     * appropriate {@link RecordWriter}, creating new writers as needed.
     *
     * <p>However, if this {@link RecordWriterManager} is already saturated with writers, and we
     * can't create a new writer, the {@link Record} is rejected and {@code false} is returned.
     */
    boolean write(Record record) {
      partitionKey.partition(record);

      if (!writers.asMap().containsKey(partitionKey) && openWriters >= maxNumWriters) {
        return false;
      }
      RecordWriter writer = fetchWriterForPartition(partitionKey);
      writer.write(record);
      return true;
    }

    /**
     * Checks if a viable {@link RecordWriter} already exists for this partition and returns it. If
     * no {@link RecordWriter} exists or if it has reached the maximum limit of bytes written, a new
     * one is created and returned.
     */
    private RecordWriter fetchWriterForPartition(PartitionKey partitionKey) {
      RecordWriter recordWriter = writers.getIfPresent(partitionKey);

      if (recordWriter == null || recordWriter.bytesWritten() > maxFileSize) {
        // calling invalidate for a non-existent key is a safe operation
        writers.invalidate(partitionKey);
        recordWriter = createWriter(partitionKey);
        writers.put(partitionKey, recordWriter);
      }
      return recordWriter;
    }

    private RecordWriter createWriter(PartitionKey partitionKey) {
      // keep track of how many writers we opened for each destination-partition path
      // use this as a prefix to differentiate the new path.
      // this avoids overwriting a data file written by a previous writer in this destination state.
      int recordIndex = writerCounts.merge(partitionKey, 1, Integer::sum);
      try {
        RecordWriter writer =
            new RecordWriter(
                catalog,
                icebergDestination,
                filePrefix + "_" + stateToken + "_" + recordIndex,
                partitionKey);
        openWriters++;
        return writer;
      } catch (IOException e) {
        throw new RuntimeException(
            String.format(
                "Encountered an error when creating a RecordWriter for table '%s', partition %s.",
                icebergDestination.getTableIdentifier(), partitionKey),
            e);
      }
    }

    private String getManifestFileLocation(PaneInfo paneInfo) {
      return FileFormat.AVRO.addExtension(
          String.format(
              "%s/metadata/%s-%s-%s.manifest",
              tableLocation, filePrefix, stateToken, paneInfo.getIndex()));
    }
  }

  private final Catalog catalog;
  private final String filePrefix;
  private final long maxFileSize;
  private final int maxNumWriters;
  @VisibleForTesting int openWriters = 0;

  @VisibleForTesting
  final Map<WindowedValue<IcebergDestination>, DestinationState> destinations = Maps.newHashMap();

  private final Map<WindowedValue<IcebergDestination>, List<ManifestFile>> totalManifestFiles =
      Maps.newHashMap();

  private boolean isClosed = false;

  RecordWriterManager(Catalog catalog, String filePrefix, long maxFileSize, int maxNumWriters) {
    this.catalog = catalog;
    this.filePrefix = filePrefix;
    this.maxFileSize = maxFileSize;
    this.maxNumWriters = maxNumWriters;
  }

  /**
   * Fetches the appropriate {@link RecordWriter} for this destination and partition and writes the
   * record.
   *
   * <p>If the {@link RecordWriterManager} is saturated (i.e. has hit the maximum limit of open
   * writers), the record is rejected and {@code false} is returned.
   */
  public boolean write(WindowedValue<IcebergDestination> icebergDestination, Row row) {
    DestinationState destinationState =
        destinations.computeIfAbsent(
            icebergDestination,
            destination -> {
              Table table = catalog.loadTable(destination.getValue().getTableIdentifier());
              return new DestinationState(destination.getValue(), table);
            });

    Record icebergRecord = IcebergUtils.beamRowToIcebergRecord(destinationState.schema, row);
    return destinationState.write(icebergRecord);
  }

  /**
   * Closes all remaining writers and collects all their {@link DataFile}s. Writes one {@link
   * ManifestFile} per windowed table destination.
   */
  @Override
  public void close() throws IOException {
    for (Map.Entry<WindowedValue<IcebergDestination>, DestinationState>
        windowedDestinationAndState : destinations.entrySet()) {
      WindowedValue<IcebergDestination> windowedDestination = windowedDestinationAndState.getKey();
      DestinationState state = windowedDestinationAndState.getValue();

      // removing writers from the state's cache will trigger the logic to collect each writer's
      // data file.
      state.writers.invalidateAll();
      if (state.dataFiles.isEmpty()) {
        continue;
      }

      OutputFile outputFile =
          state.fileIO.newOutputFile(state.getManifestFileLocation(windowedDestination.getPane()));

      ManifestWriter<DataFile> manifestWriter;
      try (ManifestWriter<DataFile> openWriter = ManifestFiles.write(state.spec, outputFile)) {
        openWriter.addAll(state.dataFiles);
        manifestWriter = openWriter;
      }
      ManifestFile manifestFile = manifestWriter.toManifestFile();

      LOG.info(
          "Successfully wrote manifest file, adding {} data files ({} rows) to table '{}': {}.",
          manifestFile.addedFilesCount(),
          manifestFile.addedRowsCount(),
          windowedDestination.getValue().getTableIdentifier(),
          outputFile.location());

      totalManifestFiles
          .computeIfAbsent(windowedDestination, dest -> Lists.newArrayList())
          .add(manifestFile);

      state.dataFiles.clear();
    }
    destinations.clear();
    checkArgument(
        openWriters == 0,
        "Expected all data writers to be closed, but found %s data writer(s) still open",
        openWriters);
    isClosed = true;
  }

  /**
   * Returns a list of accumulated windowed {@link ManifestFile}s for each windowed {@link
   * IcebergDestination}. The {@link RecordWriterManager} must first be closed before this is
   * called.
   */
  public Map<WindowedValue<IcebergDestination>, List<ManifestFile>> getManifestFiles() {
    checkState(
        isClosed,
        "Please close this %s before retrieving its manifest files.",
        getClass().getSimpleName());
    return totalManifestFiles;
  }
}