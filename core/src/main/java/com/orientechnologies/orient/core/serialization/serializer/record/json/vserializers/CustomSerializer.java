package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.*;

public class CustomSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (final ObjectOutputStream objectStream = new ObjectOutputStream(baos)) {
        objectStream.writeObject(value);
        objectStream.flush();

        generator.writeBinary(baos.toByteArray());
      }
    }
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    final Object result;
    final byte[] value = parser.getBinaryValue();
    try (final ByteArrayInputStream bais = new ByteArrayInputStream(value)) {
      try (final ObjectInputStream objectStream = new ObjectInputStream(bais)) {
        try {
          result = objectStream.readObject();
        } catch (final ClassNotFoundException e) {
          throw OException.wrapException(
              new ODatabaseException("Error during deserialization of the field"), e);
        }
      }
    }

    return result;
  }

  @Override
  public String typeId() {
    return SerializerIDs.CUSTOM;
  }

  @Override
  public JsonToken[] startTokens() {
    return new JsonToken[] {JsonToken.VALUE_STRING};
  }
}
