package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class DoubleSerializer implements ValueSerializer {
  @Override
  public void toJSON(final JsonGenerator generator, final Object value) throws IOException {
    generator.writeNumber((double) value);
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    return parser.getDoubleValue();
  }

  @Override
  public String typeId() {
    return SerializerIDs.DOUBLE;
  }

  @Override
  public JsonToken[] startTokens() {
    return new JsonToken[] {JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_INT};
  }
}
