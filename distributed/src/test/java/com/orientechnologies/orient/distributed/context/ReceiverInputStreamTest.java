package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertArrayEquals;

import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Test;

public class ReceiverInputStreamTest {

  @Test
  public void testReceive() throws IOException, InterruptedException, ExecutionException {
    OSyncState sync = new OSyncState(null, null, null, null, null);
    OReceiverInputStream stream = new OReceiverInputStream((a, b) -> {}, sync);
    var future = backgroundReceiver(stream);
    var orig = new byte[10];
    for (byte i = 0; i < 10; i++) {
      stream.receive(new byte[] {i}, i, i == 9);
      orig[i] = i;
    }
    assertArrayEquals(orig, future.get());
    stream.close();
  }

  @Test
  public void testReceiveDuplicate() throws IOException, InterruptedException, ExecutionException {
    OSyncState sync = new OSyncState(null, null, null, null, null);
    OReceiverInputStream stream = new OReceiverInputStream((a, b) -> {}, sync);
    var future = backgroundReceiver(stream);
    var orig = new byte[10];
    for (byte i = 0; i < 10; i++) {
      stream.receive(new byte[] {i}, i, i == 9);
      if (i % 3 == 0) {
        stream.receive(new byte[] {i}, i, i == 9);
      }
      orig[i] = i;
    }
    assertArrayEquals(orig, future.get());
    stream.close();
  }

  @Test
  public void testJumpingReceive() throws IOException, InterruptedException, ExecutionException {
    OSyncState sync = new OSyncState(null, null, null, null, null);
    OReceiverInputStream stream = new OReceiverInputStream((a, b) -> {}, sync);
    var future = backgroundReceiver(stream);
    var orig = new byte[10];
    for (byte i = 0; i < 10; i += 2) {
      stream.receive(new byte[] {i}, i, i == 9);
      orig[i] = i;
    }
    for (byte i = 1; i < 10; i += 2) {
      stream.receive(new byte[] {i}, i, i == 9);
      orig[i] = i;
    }
    assertArrayEquals(orig, future.get());
    stream.close();
  }

  @Test
  public void testMissOneReceive() throws IOException, InterruptedException, ExecutionException {
    OSyncState sync = new OSyncState(null, null, null, null, null);
    OReceiverInputStream stream = new OReceiverInputStream((a, b) -> {}, sync);
    var future = backgroundReceiver(stream);
    var orig = new byte[10];
    for (byte i = 0; i < 10; i++) {
      if (i != 2) {
        stream.receive(new byte[] {i}, i, i == 9);
        orig[i] = i;
      }
    }
    orig[2] = 2;
    stream.receive(new byte[] {2}, 2, false);

    assertArrayEquals(orig, future.get());
    stream.close();
  }

  @Test
  public void testMissInverted() throws IOException, InterruptedException, ExecutionException {
    OSyncState sync = new OSyncState(null, null, null, null, null);
    OReceiverInputStream stream = new OReceiverInputStream((a, b) -> {}, sync);
    var future = backgroundReceiver(stream);
    var orig = new byte[10];
    for (byte i = 0; i < 7; i++) {
      stream.receive(new byte[] {i}, i, i == 9);
      orig[i] = i;
    }
    orig[9] = 9;
    stream.receive(new byte[] {9}, 9, false);
    orig[8] = 8;
    stream.receive(new byte[] {8}, 8, false);
    orig[7] = 7;
    stream.receive(new byte[] {7}, 7, false);

    assertArrayEquals(orig, future.get());
    stream.close();
  }

  private Future<byte[]> backgroundReceiver(InputStream stream) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    new Thread(
            () -> {
              var bytes = new byte[10];
              try {
                stream.read(bytes);
              } catch (IOException e) {
                e.printStackTrace();
              }
              future.complete(bytes);
            })
        .start();
    return future;
  }
}
