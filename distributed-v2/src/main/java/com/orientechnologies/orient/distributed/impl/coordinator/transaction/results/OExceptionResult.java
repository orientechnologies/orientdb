package com.orientechnologies.orient.distributed.impl.coordinator.transaction.results;

import com.orientechnologies.common.log.OLogManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class OExceptionResult implements OTransactionResult {

  private Exception exception;

  public OExceptionResult(Exception exception) {
    this.exception = exception;
  }

  public OExceptionResult() {}

  public Exception getException() {
    return exception;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
    objectOutputStream.writeObject(exception);
    objectOutputStream.flush();
    byte[] bytes = out.toByteArray();
    output.writeInt(bytes.length);
    output.write(bytes);
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
