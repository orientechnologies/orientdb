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
    // we bound to use Object as a key for map because of broken type system
    // but key is always String
    final Map<Object, Object> map;
    if (owner != null) {
      map = new OTrackedMap<>(owner);
    } else {
      map = new HashMap<>();
    }

    final SerializerFactory serializerFactory = SerializerFactory.INSTANCE;

    JsonToken token = parser.nextToken();
    while (token != JsonToken.END_OBJECT) {
      final String key = parser.getValueAsString();

      final JsonToken valueToken = parser.nextToken();
      final Object item;

      final ValueSerializer serializer = serializerFactory.findSerializer(valueToken);
      if (serializer == null) {
        item = null;
      } else {
        item = serializer.fromJSON(parser, null);
      }

      if (map instanceof OTrackedMap) {
        ((OTrackedMap<Object>) map).putInternal(key, item);
      } else {
        map.put(key, item);
      }

      token = parser.nextToken();
    }

    return map;
  }

  @Override
  public String typeId() {
    return SerializerIDs.EMBEDDED_MAP;
  }

  @Override
  public JsonToken startToken() {
    return JsonToken.START_OBJECT;
  }
}
