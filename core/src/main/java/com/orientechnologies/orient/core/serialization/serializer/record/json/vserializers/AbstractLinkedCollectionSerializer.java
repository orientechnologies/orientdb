package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractLinkedCollectionSerializer {
    protected static void writeLinkCollection(
            final JsonGenerator generator, final Iterable<OIdentifiable> values) throws IOException {
        if (values == null) {
            generator.writeNull();
            return;
        }

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
                linkSerializer.toJSON(generator, item);
            }

        } finally {
            if (disabledAutoConversion) {
                ((ORecordLazyMultiValue) values).setAutoConvertToRecord(true);
            }
        }
    }
}
