package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class BinarySerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    generator.writeBinary((byte[]) value);
  }

  @Override
  public Object fromJSON(final JsonParser parser, ODocument owner) throws IOException {
    return parser.getBinaryValue();
  }

  @Override
  public String typeId() {
    return SerializerIDs.BINARY;
  }
}
