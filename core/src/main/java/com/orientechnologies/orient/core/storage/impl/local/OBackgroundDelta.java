package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.*;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;

public class OBackgroundDelta implements Runnable, OSyncSource {
  private OAbstractPaginatedStorage storage;
  private PipedOutputStream         outputStream;
  private InputStream               inputStream;
  private OCommandOutputListener    outputListener;
  private SortedSet<ORID>           sortedRids;
  private OLogSequenceNumber        lsn;
  private OLogSequenceNumber        endLsn;
  private CountDownLatch            finished = new CountDownLatch(1);

  public OBackgroundDelta(OAbstractPaginatedStorage storage, OCommandOutputListener outputListener, SortedSet<ORID> sortedRids,
      OLogSequenceNumber lsn, OLogSequenceNumber endLsn) throws IOException {
    this.storage = storage;
    this.outputListener = outputListener;
    this.sortedRids = sortedRids;
    this.lsn = lsn;
    this.endLsn = endLsn;
    outputStream = new PipedOutputStream();
    inputStream = new PipedInputStream(outputStream);
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.start();
  }

  public OBackgroundDelta(OLogSequenceNumber endLsn) {
    this.endLsn = endLsn;
  }

  @Override
  public void run() {
    try {
      storage.serializeDeltaContent(outputStream, outputListener, sortedRids, lsn);
    } finally {
      finished.countDown();
    }
  }

  @Override
  public boolean getIncremental() {
    return false;
  }

  @Override
  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public CountDownLatch getFinished() {
    return finished;
  }

  public OLogSequenceNumber getEndLsn() {
    return endLsn;
  }

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public void invalidate() {
    //DO NOTHING IS INVALID BY DEFINITION
  }
}
