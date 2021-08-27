package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class ShortSerializer implements ValueSerializer {
    @Override
    public void toJSON(JsonGenerator generator, Object value) throws IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }

        generator.writeNumber((short) value);
    }

    @Override
    public String typeId() {
        return "s";
    }
}
