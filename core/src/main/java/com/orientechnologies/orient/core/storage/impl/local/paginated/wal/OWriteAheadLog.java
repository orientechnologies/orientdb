/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

/**
 * @author Andrey Lomakin
 * @since 25.04.13
 */
public class OWriteAheadLog {
  private final OLocalPaginatedStorage  storage;

  private long                          lastLsn;
  private long                          nextLsn;

  private long                          masterRecord;

  private final Object                  syncObject  = new Object();

  private int                           recordsCacheSize;
  private final Queue<RecordCacheEntry> records     = new LinkedList<RecordCacheEntry>();

  private final SortedSet<LogSegment>   logSegments = new TreeSet<LogSegment>();

  private final int                     maxRecordsCacheSize;
  private final int                     commitDelay;

  private final long                    maxSegmentSize;
  private final long                    maxLogSize;

  private long                          logSize;

  private final File                    walLocation;

  public OWriteAheadLog(OLocalPaginatedStorage storage) throws IOException {
    try {
      this.storage = storage;

      String walPath = OGlobalConfiguration.WAL_LOCATION.getValueAsString();
      if (walPath == null)
        walPath = storage.getStoragePath();

      walLocation = new File(walPath);

      File[] walFiles = walLocation.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return LogSegment.validateName(name);
        }
      });

      if (walFiles.length == 0) {
        logSegments.add(new LogSegment(new File(walLocation, getSegmentName(0)), 0));

        lastLsn = -1;
        masterRecord = -1;

        logSize = 0;
      } else {
        for (File walFile : walFiles) {
          LogSegment logSegment = new LogSegment(walFile);
          logSegments.add(logSegment);
          logSize += logSegment.size();
        }

        readLastLSNAndMasterRecord();
      }

      maxRecordsCacheSize = OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger();
      commitDelay = OGlobalConfiguration.WAL_COMMIT_TIMEOUT.getValueAsInteger();
      maxSegmentSize = OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsInteger() * 1024L * 1024L;
      maxLogSize = OGlobalConfiguration.WAL_MAX_SIZE.getValueAsInteger() * 1024L * 1024L;

    } catch (FileNotFoundException e) {
      // never happened
      OLogManager.instance().error(this, "Error during file initialization for storage %s", e, storage.getName());
      throw new IllegalStateException("Error during file initialization for storage" + storage.getName(), e);
    }
  }

  private String getSegmentName(int order) {
    return this.storage.getName() + "." + order + ".wal";
  }

  public void logCheckPointStart() {
    synchronized (syncObject) {

    }
  }

  public void logCheckPointEnd() {
    synchronized (syncObject) {

    }
  }

  public void logRecord(OWALRecord operation) throws IOException {
    synchronized (syncObject) {
      byte[] serializedForm = OWALRecordsFactory.INSTANCE.toStream(operation);
      final int serializedSize = calculateSerializedSize(serializedForm);

      if (lastLsn < 0) {
        lastLsn = 0;
        nextLsn = serializedSize;
      } else {
        lastLsn = nextLsn;
        nextLsn = lastLsn + serializedSize;
      }

      if (operation.isUpdateMasterRecord())
        masterRecord = lastLsn;

      records.add(new RecordCacheEntry(serializedForm, lastLsn, masterRecord));

      recordsCacheSize += serializedSize;

      if (recordsCacheSize >= maxRecordsCacheSize)
        flushWALCache();
    }
  }

  private int calculateSerializedSize(byte[] serializedForm) {
    return 2 * OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE + serializedForm.length;
  }

  public void logDirtyPages(Set<Long> dirtyPages) {
    synchronized (syncObject) {

    }
  }

  public void logPage(OLocalPage localPage) {
    synchronized (syncObject) {

    }
  }

  public long getLastLsn() {
    synchronized (syncObject) {
      return lastLsn;
    }
  }

  public long getMasterRecord() {
    synchronized (syncObject) {
      return masterRecord;
    }
  }

  private void flushWALCache() throws IOException {
    synchronized (syncObject) {
      LogSegment logSegment = logSegments.last();
      for (RecordCacheEntry logEntry : records) {
        final int serializedSize = calculateSerializedSize(logEntry.record);

        if (logSegment.size() + serializedSize > maxSegmentSize)
          logSegment = new LogSegment(new File(walLocation, getSegmentName(logSegment.getOrder() + 1)), nextLsn);

        if (logSize + serializedSize > maxLogSize) {
          LogSegment firstSegment = logSegments.first();
          long segmentSize = firstSegment.size();
          if (!firstSegment.delete())
            OLogManager.instance().error(this, "File %s can to be deleted. Please check access rights.", firstSegment.getPath());

          logSize -= segmentSize;
          logSegments.remove(logSegment);
        }

        logSegment.logRecord(logEntry);

        logSize += serializedSize;
      }
    }
  }

  private void readLastLSNAndMasterRecord() throws IOException {
    LogSegment logSegment = logSegments.last();

    lastLsn = logSegment.readLastLsn();
    masterRecord = logSegment.readLastMasterRecord();
  }

  private final class RecordCacheEntry {
    private final byte[] record;

    private final long   lsn;
    private final long   masterRecord;

    private RecordCacheEntry(byte[] record, long lsn, long masterRecord) {
      this.record = record;
      this.lsn = lsn;
      this.masterRecord = masterRecord;
    }
  }

  private static final class LogSegment implements Comparable<LogSegment> {
    private static final int       START_LSN_OFFSET = OLongSerializer.LONG_SIZE;

    private final RandomAccessFile rndFile;
    private final File             file;
    private final long             startLSN;

    private LogSegment(File file, long startLSN) throws IOException {
      this.file = file;
      rndFile = new RandomAccessFile(file, "rws");
      rndFile.writeLong(startLSN);
      this.startLSN = startLSN;
    }

    private LogSegment(File file) throws IOException {
      this.file = file;
      rndFile = new RandomAccessFile(file, "rws");
      rndFile.seek(0);
      startLSN = rndFile.readLong();
    }

    private int getOrder() {
      return extractOrder(file.getName());
    }

    private static boolean validateName(String name) {
      if (!name.toLowerCase().endsWith(".wal"))
        return false;

      int walOrderStartIndex = name.indexOf('.');

      if (walOrderStartIndex == name.length() - 4)
        return false;

      int walOrderEndIndex = name.indexOf(walOrderStartIndex + 1, '.');
      String walOrder = name.substring(walOrderStartIndex + 1, walOrderEndIndex);
      try {
        Integer.parseInt(walOrder);
      } catch (NumberFormatException e) {
        return false;
      }

      return true;
    }

    private int extractOrder(String name) {
      int walOrderStartIndex = name.indexOf('.') + 1;

      int walOrderEndIndex = name.indexOf(walOrderStartIndex, '.');
      String walOrder = name.substring(walOrderStartIndex, walOrderEndIndex);
      try {
        return Integer.parseInt(walOrder);
      } catch (NumberFormatException e) {
        // never happen
        throw new IllegalStateException(e);
      }
    }

    @Override
    public int compareTo(LogSegment other) {
      int order = extractOrder(file.getName());
      int otherOrder = extractOrder(other.file.getName());

      return Integer.compare(order, otherOrder);
    }

    public long size() throws IOException {
      return rndFile.length();
    }

    public boolean delete() throws IOException {
      rndFile.close();
      return file.delete();
    }

    public String getPath() {
      return file.getAbsolutePath();
    }

    public void logRecord(RecordCacheEntry logEntry) throws IOException {
      long pos = rndFile.length();
      assert pos == logEntry.lsn - startLSN - START_LSN_OFFSET;

      rndFile.seek(pos);

      rndFile.write(logEntry.record);
      rndFile.writeLong(logEntry.lsn);
      rndFile.writeLong(logEntry.masterRecord);
    }

    public long readLastMasterRecord() throws IOException {
      rndFile.seek(rndFile.length() - OLongSerializer.LONG_SIZE);
      return rndFile.readLong();
    }

    public long readLastLsn() throws IOException {
      rndFile.seek(rndFile.length() - 2 * OLongSerializer.LONG_SIZE);
      return rndFile.readLong();
    }

  }
}
