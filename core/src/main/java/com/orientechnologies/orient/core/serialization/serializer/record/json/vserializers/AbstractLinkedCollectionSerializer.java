package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;

import java.io.IOException;

public abstract class AbstractLinkedCollectionSerializer {
  protected static void writeLinkCollection(
      final JsonGenerator generator, final Iterable<OIdentifiable> values) throws IOException {
    final LinkSerializer linkSerializer =
        (LinkSerializer) SerializerFactory.INSTANCE.findSerializer(OType.LINK);

    final boolean disabledAutoConversion =
        values instanceof ORecordLazyMultiValue
            && ((ORecordLazyMultiValue) values).isAutoConvertToRecord();

    if (disabledAutoConversion) {
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) values).setAutoConvertToRecord(false);
    }

    generator.writeStartArray();
    try {
      for (OIdentifiable item : values) {
        if (item == null) {
          generator.writeNull();
          continue;
        }

        linkSerializer.toJSON(generator, item);
      }

    } finally {
      if (disabledAutoConversion) {
        ((ORecordLazyMultiValue) values).setAutoConvertToRecord(true);
      }
      generator.writeEndArray();
    }
  }

  protected static <T extends OTrackedMultiValue<?, OIdentifiable>> void readLinkCollection(
      final JsonParser parser, final T collection) throws IOException {
    final LinkSerializer linkSerializer =
        (LinkSerializer) SerializerFactory.INSTANCE.findSerializer(OType.LINK);

    JsonToken nextToken = parser.nextToken();
    while (nextToken != JsonToken.END_ARRAY) {
      if (JsonToken.VALUE_NULL == nextToken) {
        collection.addInternal(null);
        continue;
      }

      if (JsonToken.VALUE_STRING != nextToken) {
        throw new ODatabaseException(
            "Invalid token was encountered : " + nextToken + ", link was expected");
      }

      final ORID rid = (ORID) linkSerializer.fromJSON(parser, null);

      collection.addInternal(rid);

      nextToken = parser.nextToken();
    }
  }
}
