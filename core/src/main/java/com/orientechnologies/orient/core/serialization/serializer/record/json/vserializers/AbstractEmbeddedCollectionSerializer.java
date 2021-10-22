package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.util.Collection;

public abstract class AbstractEmbeddedCollectionSerializer {
  protected static void writeEmbeddedCollection(
      final JsonGenerator generator, final Iterable<?> values) throws IOException {
    generator.writeStartArray();
    try {
      for (final Object element : values) {
        if (element == null) {
          generator.writeNull();
          continue;
        }

        final ValueSerializer serializer;

        OType type = OType.getTypeByValue(element);
        if (type == OType.LINK
            && (element instanceof ODocument)
            && !((OIdentifiable) element).getIdentity().isValid()) {
          type = OType.EMBEDDED;
        }

        serializer = SerializerFactory.INSTANCE.findSerializer(type);

        serializer.toJSON(generator, element);
      }
    } finally {
      generator.writeEndArray();
    }
  }

  protected static void readEmbeddedCollection(
      final JsonParser parser, final Collection<Object> collection) throws IOException {
    final SerializerFactory serializerFactory = SerializerFactory.INSTANCE;

    final boolean trackedMultiValue = collection instanceof OTrackedMultiValue;

    JsonToken nextToken = parser.nextToken();
    while (nextToken != JsonToken.END_ARRAY) {
      final ValueSerializer serializer = serializerFactory.findSerializer(nextToken);
      final Object item;

      if (serializer == null) {
        item = null;
      } else {
        item = serializer.fromJSON(parser, null);
      }

      if (trackedMultiValue) {
        //noinspection unchecked
        ((OTrackedMultiValue<?, Object>) collection).addInternal(item);
      } else {
        collection.add(item);
      }

      nextToken = parser.nextToken();
    }
  }
}
