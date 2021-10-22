package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.math.BigDecimal;

public class DecimalSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    generator.writeNumber((BigDecimal) value);
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    return parser.getDecimalValue();
  }

  @Override
  public String typeId() {
    return SerializerIDs.DECIMAL;
  }

  @Override
  public JsonToken startToken() {
    return JsonToken.VALUE_NUMBER_FLOAT;
  }
}
