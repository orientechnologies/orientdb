package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EmbeddedMapSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    final Map<?, ?> map = (Map<?, ?>) value;

    generator.writeStartObject();
    try {
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        final String key = entry.getKey().toString();
        final Object item = entry.getValue();

        generator.writeFieldName(key);
        if (item == null) {
          generator.writeNull();
        } else {
          final OType type = OType.getTypeByValue(item);
          final ValueSerializer serializer = SerializerFactory.INSTANCE.findSerializer(type);
          serializer.toJSON(generator, item);
        }
      }

    } finally {
      generator.writeStartObject();
    }
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    JsonToken nextToken = parser.nextToken();
    if (nextToken != JsonToken.START_OBJECT) {
      throw new ODatabaseException("Invalid JSON format, start of object was expected");
    }

    // we bound to use Object as a key for map because of broken type system
    // but key is always String
    final Map<Object, Object> map;
    if (owner != null) {
      map = new OTrackedMap<>(owner);
    } else {
      map = new HashMap<>();
    }

    final SerializerFactory serializerFactory = SerializerFactory.INSTANCE;

    nextToken = parser.nextToken();
    while (nextToken != JsonToken.END_OBJECT) {
      final String fieldName = parser.nextFieldName();

      final JsonToken valueToken = parser.nextToken();
      final Object item;
      if (valueToken == JsonToken.VALUE_NULL) {
        item = null;
      } else if (valueToken.isNumeric()) {
        final ValueSerializer longSerializer = serializerFactory.findSerializer(OType.LONG);
        item = longSerializer.fromJSON(parser, null);
      } else if (valueToken.isBoolean()) {
        final ValueSerializer booleanSerializer = serializerFactory.findSerializer(OType.BOOLEAN);
        item = booleanSerializer.fromJSON(parser, null);
      } else if (valueToken == JsonToken.START_OBJECT) {
        final RecordSerializer recordSerializer = RecordSerializer.INSTANCE;
        item = recordSerializer.fromJSON(parser, null);
      } else if (valueToken == JsonToken.START_ARRAY) {
        final ValueSerializer listSerializer = serializerFactory.findSerializer(OType.EMBEDDEDLIST);
        item = listSerializer.fromJSON(parser, null);
      } else if (valueToken == JsonToken.VALUE_STRING) {
        final ValueSerializer stringSerializer = serializerFactory.findSerializer(OType.STRING);
        item = stringSerializer.fromJSON(parser, null);
      } else {
        throw new ODatabaseException("Invalid type of JSON token encountered " + nextToken);
      }

      map.put(fieldName, item);

      nextToken = parser.nextToken();
    }

    return map;
  }

  @Override
  public String typeId() {
    return SerializerIDs.EMBEDDED_MAP;
  }
}
