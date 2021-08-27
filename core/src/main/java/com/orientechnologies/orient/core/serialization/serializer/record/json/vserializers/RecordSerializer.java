package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public final class RecordSerializer implements ValueSerializer {
    public static final RecordSerializer INSTANCE = new RecordSerializer();

    @Override
    public void toJSON(JsonGenerator generator, Object value) throws IOException {

    }

    @Override
    public String typeId() {
        return null;
    }
}
