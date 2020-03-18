package com.orientechnologies.orient.core.storage.impl.local;

import java.io.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class OBackgroundNewDelta implements Runnable, OSyncSource {
  private List<OTransactionData> transactions;
  private PipedOutputStream      outputStream;
  private InputStream            inputStream;
  private CountDownLatch         finished = new CountDownLatch(1);

  public OBackgroundNewDelta(List<OTransactionData> transactions) throws IOException {
    this.transactions = transactions;
    outputStream = new PipedOutputStream();
    inputStream = new PipedInputStream(outputStream);
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void run() {
    try {
      DataOutput output = new DataOutputStream(outputStream);
      for (OTransactionData transaction : transactions) {
        transaction.getTransactionId().write(output);
        output.writeInt(transaction.getChanges().size());
        for (byte[] change : transaction.getChanges()) {
          output.writeInt(change.length);
          output.write(change, 0, change.length);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
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
    //DO NOTHING IS INVALID BY DEFINITION
  }
}
