package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@SuppressWarnings("serial")
public class OSerializableWrapper implements OSerializableStream {

  private Serializable serializable;

  public OSerializableWrapper() {}

  public OSerializableWrapper(Serializable ser) {
    this.serializable = ser;
  }

  @Override
  public byte[] toStream() throws OSerializationException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      ObjectOutputStream writer = new ObjectOutputStream(output);
      writer.writeObject(serializable);
      writer.close();
    } catch (IOException e) {
      throw OException.wrapException(
          new ODatabaseException("Error on serialization of Serializable"), e);
    }
    return output.toByteArray();
  }

  @Override
  public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
    ByteArrayInputStream stream = new ByteArrayInputStream(iStream);
    try {
      ObjectInputStream reader = new ObjectInputStream(stream);
      serializable = (Serializable) reader.readObject();
      reader.close();
    } catch (Exception e) {
      throw OException.wrapException(
          new ODatabaseException("Error on deserialization of Serializable"), e);
    }
    return this;
  }

  public Serializable getSerializable() {
    return serializable;
  }
}
