package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;


public abstract class AbstractEmbeddedCollectionSerializer {
    protected static void writeEmbeddedCollection(
            final JsonGenerator generator, final Iterable<?> values) throws IOException {
        if (values == null) {
            generator.writeNull();
            return;
        }

        generator.writeStartArray();
        try {
            for (final Object element : values) {
                if (element == null) {
                    generator.writeNull();
                    continue;
                }

                final ValueSerializer serializer;
                if (element instanceof ORecord) {
                    serializer = RecordSerializer.INSTANCE;
                } else {
                    final OType type = OType.getTypeByValue(element);
                    serializer = SerializerFactory.INSTANCE.findSerializer(type);
                }

                serializer.toJSON(generator, element);
            }
        } finally {
            generator.writeEndArray();
        }
    }
}
