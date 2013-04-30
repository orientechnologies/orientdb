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

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

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
  private static final long      ONE_KB               = 1024L;
  private OLogSequenceNumber     lastCheckpoint;

  private final Object           syncObject           = new Object();

  private final List<LogSegment> logSegments          = new ArrayList<LogSegment>();
  private final RandomAccessFile masterRecordLSNHolder;
  private boolean                useFirstMasterRecord = true;

  private final int              maxRecordsCacheSize;
  private final int              commitDelay;

  private final long             maxSegmentSize;
  private final long             maxLogSize;

  private long                   logSize;

  private final File             walLocation;
  private final String           storageName;
  private File                   masterRecordFile;
  private OLogSequenceNumber     firstMasterRecord;
  private OLogSequenceNumber     secondMasterRecord;

  private static String calculateWalPath(OLocalPaginatedStorage storage) {
    String walPath = OGlobalConfiguration.WAL_LOCATION.getValueAsString();
    if (walPath == null)
      walPath = storage.getStoragePath();

    return walPath;
  }

  public OWriteAheadLog(OLocalPaginatedStorage storage) throws IOException {
    this(OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger(), OGlobalConfiguration.WAL_COMMIT_TIMEOUT.getValueAsInteger(),
        OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsInteger() * ONE_KB * ONE_KB, OGlobalConfiguration.WAL_MAX_SIZE
            .getValueAsInteger() * ONE_KB * ONE_KB, storage.getName(), calculateWalPath(storage));
  }

  public OWriteAheadLog(int maxRecordsCacheSize, int commitDelay, long maxSegmentSize, long maxLogSize, String storageName,
      String walPath) throws IOException {
    this.maxRecordsCacheSize = maxRecordsCacheSize;
    this.commitDelay = commitDelay;
    this.maxSegmentSize = maxSegmentSize;
    this.maxLogSize = maxLogSize;
    this.storageName = storageName;

    try {
      this.walLocation = new File(walPath);

      File[] walFiles = this.walLocation.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return validateName(name);
        }
      });

      if (walFiles.length == 0) {
        logSegments.add(new LogSegment(new File(this.walLocation, getSegmentName(0)), maxRecordsCacheSize));

        logSize = 0;
      } else {
        for (File walFile : walFiles) {
          LogSegment logSegment = new LogSegment(walFile, maxRecordsCacheSize);
          logSegments.add(logSegment);
          logSize += logSegment.size();
        }

        Collections.sort(logSegments);
      }

      masterRecordFile = new File(walLocation, storageName + ".wmr");
      masterRecordLSNHolder = new RandomAccessFile(masterRecordFile, "rwd");

      if (masterRecordLSNHolder.length() > 0) {
        firstMasterRecord = readMasterRecord(storageName, 0);
        secondMasterRecord = readMasterRecord(storageName, 1);

        if (firstMasterRecord == null) {
          useFirstMasterRecord = true;
          lastCheckpoint = secondMasterRecord;
        } else if (secondMasterRecord == null) {
          useFirstMasterRecord = false;
          lastCheckpoint = firstMasterRecord;
        } else {
          if (firstMasterRecord.compareTo(secondMasterRecord) >= 0) {
            lastCheckpoint = firstMasterRecord;
            useFirstMasterRecord = false;
          } else {
            lastCheckpoint = secondMasterRecord;
            useFirstMasterRecord = true;
          }
        }
      }
    } catch (FileNotFoundException e) {
      // never happened
      OLogManager.instance().error(this, "Error during file initialization for storage %s", e, storageName);
      throw new IllegalStateException("Error during file initialization for storage " + storageName, e);
    }
  }

  public File getWalLocation() {
    return walLocation;
  }

  public void restore() throws IOException {
    synchronized (syncObject) {
      for (LogSegment logSegment : logSegments) {
        logSegment.restore();
      }

      fixSize();
      fixMasterRecords();
    }
  }

  private void fixMasterRecords() throws IOException {
    if (firstMasterRecord != null) {
      int index = firstMasterRecord.getSegment() - logSegments.get(0).getOrder();
      if (logSegments.size() <= index) {
        firstMasterRecord = null;
      } else {
        LogSegment firstMasterRecordSegment = logSegments.get(index);
        if (firstMasterRecordSegment.size() <= firstMasterRecord.getPosition())
          firstMasterRecord = null;
      }
    }

    if (secondMasterRecord != null) {
      int index = secondMasterRecord.getSegment() - logSegments.get(0).getOrder();
      if (logSegments.size() <= index) {
        secondMasterRecord = null;
      } else {
        LogSegment secondMasterRecordSegment = logSegments.get(index);
        if (secondMasterRecordSegment.size() <= secondMasterRecord.getPosition())
          secondMasterRecord = null;
      }
    }

    if (firstMasterRecord == null && secondMasterRecord == null) {
      masterRecordLSNHolder.setLength(0);
      masterRecordLSNHolder.getFD().sync();
      lastCheckpoint = null;
    } else {
      if (secondMasterRecord == null)
        secondMasterRecord = firstMasterRecord;
      else
        firstMasterRecord = secondMasterRecord;

      lastCheckpoint = firstMasterRecord;

      writeMasterRecord(0, firstMasterRecord);
      writeMasterRecord(1, secondMasterRecord);
    }
  }

  private void fixSize() throws IOException {
    logSize = 0;
    for (LogSegment logSegment : logSegments) {
      logSize += logSegment.size();
    }
  }

  private OLogSequenceNumber readMasterRecord(String storageName, int index) throws IOException {
    CRC32 crc32 = new CRC32();
    try {
      masterRecordLSNHolder.seek(index * (2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE));

      int firstCRC = masterRecordLSNHolder.readInt();
      int segment = masterRecordLSNHolder.readInt();
      long position = masterRecordLSNHolder.readLong();

      byte[] serializedLSN = new byte[OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE];
      OIntegerSerializer.INSTANCE.serialize(segment, serializedLSN, 0);
      OLongSerializer.INSTANCE.serialize(position, serializedLSN, OIntegerSerializer.INT_SIZE);
      crc32.update(serializedLSN);

      if (firstCRC != ((int) crc32.getValue())) {
        OLogManager.instance().error(this, "Can not restore %d WAL master record for storage %s crc check is failed", index,
            storageName);
        return null;
      }

      return new OLogSequenceNumber(segment, position);
    } catch (EOFException eofException) {
      OLogManager.instance().warn(this, "Can not restore %d WAL master record for storage %s", index, storageName);
      return null;
    }
  }

  private void writeMasterRecord(int index, OLogSequenceNumber masterRecord) throws IOException {
    masterRecordLSNHolder.seek(index * (2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE));
    CRC32 crc32 = new CRC32();

    byte[] serializedLSN = new byte[OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE];
    OIntegerSerializer.INSTANCE.serialize(masterRecord.getSegment(), serializedLSN, 0);
    OLongSerializer.INSTANCE.serialize(masterRecord.getPosition(), serializedLSN, OIntegerSerializer.INT_SIZE);
    crc32.update(serializedLSN);

    masterRecordLSNHolder.writeInt((int) crc32.getValue());
    masterRecordLSNHolder.writeInt(masterRecord.getSegment());
    masterRecordLSNHolder.writeLong(masterRecord.getPosition());
  }

  private String getSegmentName(int order) {
    return storageName + "." + order + ".wal";
  }

  public OLogSequenceNumber logCheckPointStart() throws IOException {
    synchronized (syncObject) {
      OCheckpointStartRecord record = new OCheckpointStartRecord();
      logRecord(record);
      return record.getLsn();
    }
  }

  public OLogSequenceNumber logCheckPointEnd() throws IOException {
    synchronized (syncObject) {
      OCheckpointEndRecord record = new OCheckpointEndRecord();
      logRecord(record);
      return record.getLsn();
    }
  }

  public OLogSequenceNumber logRecord(OWALRecord record) throws IOException {
    synchronized (syncObject) {
      final byte[] serializedForm = OWALRecordsFactory.INSTANCE.toStream(record);
      final int entrySize = calculateEntrySize(serializedForm);

      if (logSize + entrySize > maxLogSize) {
        LogSegment first = logSegments.get(0);
        logSize -= first.size();

        if (!first.delete())
          OLogManager.instance().error(this, "Log segment %s can not be removed from WAL", first.getPath());

        logSegments.remove(0);

        fixMasterRecords();
      }

      LogSegment last = logSegments.get(logSegments.size() - 1);

      if (last.size() + entrySize > maxSegmentSize) {
        last.flush();

        last = new LogSegment(new File(walLocation, getSegmentName(last.getOrder() + 1)), maxRecordsCacheSize);
        logSegments.add(last);
      }

      final OLogSequenceNumber lsn = last.logRecord(serializedForm, record.isUpdateMasterRecord());
      record.setLsn(lsn);

      if (record.isUpdateMasterRecord())
        lastCheckpoint = lsn;

      logSize += entrySize;

      return lsn;
    }
  }

  public long size() {
    synchronized (syncObject) {
      return logSize;
    }
  }

  public void close() throws IOException {
    synchronized (syncObject) {
      for (LogSegment logSegment : logSegments)
        logSegment.close();
      masterRecordLSNHolder.close();
    }
  }

  public void delete() throws IOException {
    synchronized (syncObject) {
      close();

      for (LogSegment logSegment : logSegments)
        if (!logSegment.delete())
          OLogManager.instance().error(this, "Can not delete WAL segment %s for storage %s", logSegment.getPath(), storageName);

      if (!masterRecordFile.delete())
        OLogManager.instance().error(this, "Can not delete WAL state file for %s storage", storageName);
    }
  }

  public void logDirtyPages(Set<ODirtyPageId> dirtyPages) {
    synchronized (syncObject) {

    }
  }

  public void logPage(OLocalPage localPage) {
    synchronized (syncObject) {

    }
  }

  public OLogSequenceNumber getLastCheckpoint() {
    synchronized (syncObject) {
      return lastCheckpoint;
    }
  }

  public OWALRecord read(OLogSequenceNumber lsn) throws IOException {
    synchronized (syncObject) {
      int segment = lsn.getSegment();
      int index = segment - logSegments.get(0).getOrder();

      if (index < 0 || index >= logSegments.size())
        return null;

      LogSegment logSegment = logSegments.get(index);
      byte[] recordEntry = logSegment.readRecord(lsn);
      if (recordEntry == null)
        return null;

      final OWALRecord record = OWALRecordsFactory.INSTANCE.fromStream(recordEntry);
      record.setLsn(lsn);

      return record;
    }
  }

  public OWALRecord readNext(OLogSequenceNumber lsn) throws IOException {
    synchronized (syncObject) {
      int order = lsn.getSegment();
      int index = order - logSegments.get(0).getOrder();

      if (index < 0 || index >= logSegments.size())
        return null;

      LogSegment logSegment = logSegments.get(index);
      OLogSequenceNumber nextLSN = logSegment.getNextLSN(lsn);

      if (nextLSN == null) {
        order++;
        index++;
        if (index >= logSegments.size())
          return null;

        nextLSN = new OLogSequenceNumber(order, 0);
      }

      return read(nextLSN);
    }
  }

  public static boolean validateName(String name) {
    if (!name.toLowerCase().endsWith(".wal"))
      return false;

    int walOrderStartIndex = name.indexOf('.');

    if (walOrderStartIndex == name.length() - 4)
      return false;

    int walOrderEndIndex = name.indexOf('.', walOrderStartIndex + 1);
    String walOrder = name.substring(walOrderStartIndex + 1, walOrderEndIndex);
    try {
      Integer.parseInt(walOrder);
    } catch (NumberFormatException e) {
      return false;
    }

    return true;
  }

  private static int calculateEntrySize(byte[] logEntry) {
    return logEntry.length + 2 * OIntegerSerializer.INT_SIZE;
  }

  private final class LogSegment implements Comparable<LogSegment> {
    private final RandomAccessFile rndFile;
    private final File             file;
    private long                   size;

    private final int              order;
    private final int              maxRecordsCacheSize;
    private boolean                closed;

    private final List<CacheEntry> records = new ArrayList<CacheEntry>();
    private int                    recordsCacheSize;

    private LogSegment(File file, int maxRecordsCacheSize) throws IOException {
      this.file = file;
      this.maxRecordsCacheSize = maxRecordsCacheSize;

      rndFile = new RandomAccessFile(file, "rw");
      size = rndFile.length();
      order = extractOrder(file.getName());
      closed = false;
    }

    public int getOrder() {
      return order;
    }

    private int extractOrder(String name) {
      int walOrderStartIndex = name.indexOf('.') + 1;

      int walOrderEndIndex = name.indexOf('.', walOrderStartIndex);
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
      final int otherOrder = other.order;

      if (order > otherOrder)
        return 1;
      else if (order < otherOrder)
        return -1;

      return 0;
    }

    public long size() throws IOException {
      return size;
    }

    public boolean delete() throws IOException {
      close();
      return file.delete();
    }

    public String getPath() {
      return file.getAbsolutePath();
    }

    public OLogSequenceNumber logRecord(byte[] record, boolean updateMasterRecord) throws IOException {
      final OLogSequenceNumber lsn = new OLogSequenceNumber(order, size);
      final int entrySize = calculateEntrySize(record);

      records.add(new CacheEntry(record, lsn, updateMasterRecord));

      size += entrySize;
      recordsCacheSize += entrySize;

      if (recordsCacheSize >= maxRecordsCacheSize)
        flushWALCache();

      return lsn;
    }

    private void flushWALCache() throws IOException {
      if (records.isEmpty()) {
        assert rndFile.length() == size;
        return;
      }

      rndFile.seek(rndFile.length());
      final CRC32 crc32 = new CRC32();

      for (CacheEntry cacheEntry : records) {
        crc32.reset();
        crc32.update(cacheEntry.record);

        long crc = crc32.getValue();

        long position = rndFile.length();

        rndFile.writeInt((int) crc);
        rndFile.writeInt(cacheEntry.record.length);
        rndFile.write(cacheEntry.record);

        if (cacheEntry.updateMasterRecord) {
          if (useFirstMasterRecord) {
            firstMasterRecord = new OLogSequenceNumber(order, position);
            writeMasterRecord(0, firstMasterRecord);
            useFirstMasterRecord = false;
          } else {
            secondMasterRecord = new OLogSequenceNumber(order, position);
            writeMasterRecord(1, secondMasterRecord);
            useFirstMasterRecord = true;
          }
        }
      }

      rndFile.getFD().sync();

      records.clear();
      recordsCacheSize = 0;

      assert rndFile.length() == size;
    }

    public byte[] readRecord(OLogSequenceNumber lsn) throws IOException {
      assert lsn.getSegment() == order;

      if (!records.isEmpty() && records.get(0).lsn.compareTo(lsn) <= 0) {
        for (CacheEntry cacheEntry : records) {
          if (cacheEntry.lsn.equals(lsn))
            return cacheEntry.record;
        }

        return null;
      }

      final long pos = lsn.getPosition();
      if (rndFile.length() <= pos)
        return null;

      byte[] record;
      try {
        rndFile.seek(pos);
        final int crc = rndFile.readInt();
        final int entrySize = rndFile.readInt();

        record = new byte[entrySize];
        rndFile.readFully(record);

        CRC32 crc32 = new CRC32();
        crc32.update(record);

        if (crc != ((int) crc32.getValue())) {
          OLogManager.instance().error(this, "WAL record with LSN %s is broken.", lsn);
          throw new OWriteAheadLogRecordIsBrokenException("WAL record with LSN " + lsn + " is broken.");
        }
      } catch (EOFException eofException) {
        OLogManager.instance().error(this, "WAL record with LSN %s is broken.", lsn, eofException);
        throw new OWriteAheadLogRecordIsBrokenException("WAL record with LSN " + lsn + " is broken.", eofException);
      }

      return record;
    }

    public OLogSequenceNumber getNextLSN(OLogSequenceNumber lsn) throws IOException {
      final byte[] record = readRecord(lsn);
      if (record == null)
        return null;

      long pos = lsn.getPosition();
      pos += calculateEntrySize(record);

      if (pos >= size)
        return null;

      return new OLogSequenceNumber(order, pos);
    }

    public void close() throws IOException {
      if (!closed) {
        flush();
        rndFile.close();
        closed = true;
      }
    }

    public void flush() throws IOException {
      flushWALCache();
    }

    public void restore() throws IOException {
      flushWALCache();

      rndFile.seek(0);
      while (rndFile.getFilePointer() < rndFile.length()) {
        long startPos = rndFile.getFilePointer();

        try {
          final int crc = rndFile.readInt();
          final int entrySize = rndFile.readInt();

          byte[] record = new byte[entrySize];
          rndFile.readFully(record);

          CRC32 crc32 = new CRC32();
          crc32.update(record);

          if (crc != ((int) crc32.getValue())) {
            rndFile.setLength(startPos);
            size = startPos;

            OLogManager.instance().warn(this,
                "In WAL segment %s broken record with LSN %s was detected, segment is size truncated to %d bytes", file.getPath(),
                new OLogSequenceNumber(order, startPos), startPos);

            break;
          }
        } catch (EOFException e) {
          rndFile.setLength(startPos);
          size = startPos;

          OLogManager.instance().warn(this,
              "In WAL segment %s broken record with LSN %s was detected, segment is size truncated to %d bytes", file.getPath(),
              new OLogSequenceNumber(order, startPos), startPos);

          break;
        }
      }
      rndFile.getFD().sync();
    }
  }

  private static final class CacheEntry {
    private final byte[]             record;
    private final OLogSequenceNumber lsn;
    private final boolean            updateMasterRecord;

    private CacheEntry(byte[] record, OLogSequenceNumber lsn, boolean updateMasterRecord) {
      this.record = record;
      this.lsn = lsn;
      this.updateMasterRecord = updateMasterRecord;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      CacheEntry that = (CacheEntry) o;

      if (updateMasterRecord != that.updateMasterRecord)
        return false;
      if (!lsn.equals(that.lsn))
        return false;
      if (!Arrays.equals(record, that.record))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(record);
      result = 31 * result + lsn.hashCode();
      result = 31 * result + (updateMasterRecord ? 1 : 0);
      return result;
    }
  }
}
