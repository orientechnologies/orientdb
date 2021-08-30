package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class LongSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    if (value == null) {
      generator.writeNull();
      return;
    }

    generator.writeNumber((long) value);
  }

  @Override
  public String typeId() {
    return SerializerIDs.LONG;
  }
}
