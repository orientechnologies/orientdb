package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.util.Map;

public class LinkMapSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    if (value == null) {
      generator.writeNull();
      return;
    }

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
  public String typeId() {
    return SerializerIDs.LINK_MAP;
  }
}
