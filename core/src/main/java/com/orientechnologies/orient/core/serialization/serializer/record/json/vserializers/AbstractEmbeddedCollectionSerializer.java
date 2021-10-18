package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
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
    JsonToken nextToken = parser.nextToken();
    if (nextToken != JsonToken.START_ARRAY) {
      throw new ODatabaseException("Invalid JSON format, start of array was expected");
    }

    final SerializerFactory serializerFactory = SerializerFactory.INSTANCE;

    nextToken = parser.nextToken();
    while (nextToken != JsonToken.END_ARRAY) {
      final Object item;
      if (nextToken == JsonToken.VALUE_NULL) {
        item = null;
      } else if (nextToken.isNumeric()) {
        final ValueSerializer longSerializer = serializerFactory.findSerializer(OType.LONG);
        item = longSerializer.fromJSON(parser, null);
      } else if (nextToken.isBoolean()) {
        final ValueSerializer booleanSerializer = serializerFactory.findSerializer(OType.BOOLEAN);
        item = booleanSerializer.fromJSON(parser, null);
      } else if (nextToken == JsonToken.START_OBJECT) {
        final RecordSerializer recordSerializer = RecordSerializer.INSTANCE;
        item = recordSerializer.fromJSON(parser, null);
      } else if (nextToken == JsonToken.START_ARRAY) {
        final ValueSerializer listSerializer = serializerFactory.findSerializer(OType.EMBEDDEDLIST);
        item = listSerializer.fromJSON(parser, null);
      } else if (nextToken == JsonToken.VALUE_STRING) {
        final ValueSerializer stringSerializer = serializerFactory.findSerializer(OType.STRING);
        item = stringSerializer.fromJSON(parser, null);
      } else {
        throw new ODatabaseException("Invalid type of JSON token encountered " + nextToken);
      }

      collection.add(item);
      nextToken = parser.nextToken();
    }
  }
}
