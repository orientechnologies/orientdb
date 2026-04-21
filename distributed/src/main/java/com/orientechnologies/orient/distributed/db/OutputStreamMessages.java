package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamMessages extends OutputStream {

  public interface MessageSender {
    void sendBuffer(OSyncState state, byte[] data, boolean finished);
  }

  private OSyncState state;
  private MessageSender sender;

  public OutputStreamMessages(MessageSender sender, OSyncState state) {
    this.state = state;
    this.sender = sender;
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // Copy every time the data because of multi-threading
    byte[] data = new byte[len];
    System.arraycopy(b, off, data, 0, len);
    this.sender.sendBuffer(state, data, false);
  }

  @Override
  public void close() throws IOException {
    this.sender.sendBuffer(state, new byte[] {}, true);
    state.close();
  }
}
