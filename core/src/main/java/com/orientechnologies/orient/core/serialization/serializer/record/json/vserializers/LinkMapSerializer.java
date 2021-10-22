package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LinkMapSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    final LinkSerializer linkSerializer =
        (LinkSerializer) SerializerFactory.INSTANCE.findSerializer(OType.LINK);

    @SuppressWarnings("unchecked")
    final Map<Object, OIdentifiable> map = (Map<Object, OIdentifiable>) value;

    final boolean disabledAutoConversion =
        map instanceof ORecordLazyMultiValue
            && ((ORecordLazyMultiValue) map).isAutoConvertToRecord();

    if (disabledAutoConversion) {
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) map).setAutoConvertToRecord(false);
    }
    generator.writeStartObject();
    try {
      for (Map.Entry<Object, OIdentifiable> entry : map.entrySet()) {
        final String key = entry.getKey().toString();
        generator.writeFieldName(key);

        if (entry.getValue() == null) {
          generator.writeNull();
          continue;
        }

        linkSerializer.toJSON(generator, entry.getValue());
      }
    } finally {
      generator.writeEndObject();

      if (disabledAutoConversion) {
        ((ORecordLazyMultiValue) map).setAutoConvertToRecord(true);
      }
    }
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {

    Map<Object, OIdentifiable> map;
    if (owner != null) {
      map = new ORecordLazyMap(owner);
    } else {
      map = new HashMap<>();
    }

    final SerializerFactory serializerFactory = SerializerFactory.INSTANCE;
    final LinkSerializer linkSerializer =
        (LinkSerializer) serializerFactory.findSerializer(OType.LINK);

    JsonToken nextToken = parser.nextToken();
    while (nextToken != JsonToken.END_OBJECT) {
      final String key = parser.getValueAsString();
      final JsonToken valueToken = parser.nextToken();

      if (valueToken == JsonToken.VALUE_NULL) {
        if (owner != null) {
          ((OTrackedMap<OIdentifiable>)map).putInternal(key, null);
        } else {
          map.put(key, null);
        }
      } else if (valueToken == JsonToken.VALUE_STRING) {
        final ORID link = (ORID) linkSerializer.fromJSON(parser, null);

        if (owner != null) {
          ((OTrackedMap<OIdentifiable>)map).putInternal(key, link);
        } else {
          map.put(key, link);
        }
      } else {
        throw new ODatabaseException("Unexpected JSON token " + valueToken + " , string was expected as a value of field " + key);
      }

      nextToken = parser.nextToken();
    }

    return map;
  }

  @Override
  public String typeId() {
    return SerializerIDs.LINK_MAP;
  }

  @Override
  public JsonToken startToken() {
    return JsonToken.START_OBJECT;
  }
}
