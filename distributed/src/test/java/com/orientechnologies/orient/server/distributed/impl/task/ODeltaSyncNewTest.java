package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertArrayEquals;

public class ODeltaSyncNewTest {

  @Test
  public void testSerializationDeserialization() throws IOException {
    byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
    OSyncDatabaseNewDeltaTask task = new OSyncDatabaseNewDeltaTask(new TestMetadataHolder(bytes));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    task.toStream(new DataOutputStream(outputStream));
    OSyncDatabaseNewDeltaTask syncTask = new OSyncDatabaseNewDeltaTask();
    syncTask.fromStream(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())), null);
    assertArrayEquals(syncTask.getLastState(), bytes);
  }

  private static class TestMetadataHolder implements OTxMetadataHolder {
    private byte[] bytes;

    public TestMetadataHolder(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public byte[] metadata() {
      return bytes;
    }

    @Override
    public void notifyMetadataRead() {

    }

    @Override
    public OTransactionId getId() {
      return null;
    }
  }
}
