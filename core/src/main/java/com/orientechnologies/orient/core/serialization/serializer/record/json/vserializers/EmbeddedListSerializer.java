package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class EmbeddedListSerializer extends AbstractEmbeddedCollectionSerializer implements ValueSerializer {
    @Override
    public void toJSON(JsonGenerator generator, Object value) throws IOException {
        writeEmbeddedCollection(generator, (Iterable<?>) value);
    }

    @Override
    public String typeId() {
        return "i";
    }
}
