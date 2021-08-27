package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class CustomSerializer implements ValueSerializer {
    @Override
    public void toJSON(JsonGenerator generator, Object value) throws IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (final ObjectOutputStream objectStream = new ObjectOutputStream(baos)) {
                objectStream.writeObject(value);
                objectStream.flush();

                generator.writeBinary(baos.toByteArray());
            }
        }
    }

    @Override
    public String typeId() {
        return "u";
    }
}
