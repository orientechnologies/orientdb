package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.math.BigDecimal;

public class DecimalSerializer implements ValueSerializer {
    @Override
    public void toJSON(JsonGenerator generator, Object value) throws IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }

        generator.writeNumber((BigDecimal) value);
    }

    @Override
    public String typeId() {
        return "c";
    }
}
