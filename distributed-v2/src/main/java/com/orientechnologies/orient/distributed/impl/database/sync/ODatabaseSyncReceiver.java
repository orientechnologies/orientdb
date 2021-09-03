package com.orientechnologies.orient.distributed.impl.database.sync;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class ODatabaseSyncReceiver {

  private final PipedOutputStream output;
  private final OrientDBDistributed orientDB;
  private final String database;
  private final boolean incremental;

  public ODatabaseSyncReceiver(
      OrientDBDistributed orientDBDistributed, String database, boolean incremental) {
    this.orientDB = orientDBDistributed;
    this.database = database;
    this.incremental = incremental;
    output = new PipedOutputStream();
  }

  public void run() {
    try {
      PipedInputStream inputStream = new PipedInputStream(output);
      new Thread(
              () -> {
                if (incremental) {
                  orientDB.fullSync(database, inputStream, null);
                } else {
                  orientDB.restore(database, inputStream, null, null, null);
                }
              })
          .start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void receive(byte[] bytes, int len) {
    try {
      this.output.write(bytes, 0, len);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
