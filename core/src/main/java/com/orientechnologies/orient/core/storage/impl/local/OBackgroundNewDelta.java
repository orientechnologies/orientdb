package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.tx.OTransactionData;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class OBackgroundNewDelta implements Runnable, OSyncSource {
  public static final int CHUNK_MAX_SIZE = 8388608; // 8MB
  private List<OTransactionData> transactions;
  private PipedOutputStream outputStream;
  private InputStream inputStream;
  private CountDownLatch finished = new CountDownLatch(1);

  public OBackgroundNewDelta(List<OTransactionData> transactions) throws IOException {
    this.transactions = transactions;
    outputStream = new PipedOutputStream();
    inputStream = new PipedInputStream(outputStream, CHUNK_MAX_SIZE);
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void run() {
    try {
      DataOutput output = new DataOutputStream(outputStream);
      for (OTransactionData transaction : transactions) {
        output.writeBoolean(true);
        transaction.write(output);
      }
      output.writeBoolean(false);
      outputStream.close();
    } catch (IOException e) {
      OLogManager.instance().debug(this, "Error on network delta serialization", e);
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

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public void invalidate() {
    // DO NOTHING IS INVALID BY DEFINITION
  }
}
