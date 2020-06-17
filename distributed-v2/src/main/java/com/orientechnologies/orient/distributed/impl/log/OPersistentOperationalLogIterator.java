package com.orientechnologies.orient.distributed.impl.log;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.NoSuchElementException;

public class OPersistentOperationalLogIterator implements OOplogIterator {
  private final Long from;
  private final long to;
  private final OPersistentOperationalLogV1 opLog;

  private long nextIdToLoad;
  private OOperationLogEntry nextEntry;
  private DataInputStream stream;

  public OPersistentOperationalLogIterator(OPersistentOperationalLogV1 opLog, Long from, long to) {
    this.opLog = opLog;
    this.from = from;
    this.to = to;
    if (from == null) {
      nextIdToLoad = 0L;
    } else {
      nextIdToLoad = from;
    }
  }

  @Override
  public boolean hasNext() {
    if (nextEntry == null) {
      loadNext();
    }
    return nextEntry != null;
  }

  @Override
  public OOperationLogEntry next() {
    if (nextEntry == null) {
      loadNext();
    }
    if (nextEntry == null) {
      throw new NoSuchElementException();
    }
    OOperationLogEntry result = nextEntry;
    nextEntry = null;
    return result;
  }

  private void loadNext() {
    nextEntry = null;
    if (nextIdToLoad > to) {
      return;
    }
    if (stream == null || nextIdToLoad % OPersistentOperationalLogV1.LOG_ENTRIES_PER_FILE == 0) {
      initStream();
      if (stream == null) {
        return;
      }
    }
    do {
      nextEntry = opLog.readRecord(stream);
    } while (from != null && nextEntry != null && nextEntry.getLogId().getId() < from);

    nextIdToLoad++;
  }

  private void initStream() {
    if (stream != null) {
      try {
        stream.close();
      } catch (IOException e) {
      }
    }
    int fileSuffix = (int) (nextIdToLoad / OPersistentOperationalLogV1.LOG_ENTRIES_PER_FILE);
    File file = new File(opLog.calculateLogFileFullPath(fileSuffix));
    if (!file.exists()) {
      return;
    }
    try {
      this.stream = new DataInputStream(new FileInputStream(file));
    } catch (FileNotFoundException e) {
      throw new IllegalStateException("Oplog file not found: " + file.getAbsolutePath());
    }
  }

  public void close() {
    try {
      if (this.stream != null) {
        this.stream.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
