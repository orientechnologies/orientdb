package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.results;

import com.orientechnologies.common.log.OLogManager;

import java.io.*;

public class OExceptionResult implements OTransactionResult {

  private Exception exception;

  public OExceptionResult(Exception exception) {
    this.exception = exception;
  }

  public OExceptionResult() {

  }

  public Exception getException() {
    return exception;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new ObjectOutputStream(out).writeObject(exception);
    byte[] bytes = out.toByteArray();
    output.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    int size = input.readInt();
    byte[] bytes = new byte[size];
    input.readFully(bytes);
    try {
      exception = (Exception) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
    } catch (ClassNotFoundException e) {
      OLogManager.instance().warn(this, "Error deserializing exception:" + e.getMessage());
    }
  }
}
