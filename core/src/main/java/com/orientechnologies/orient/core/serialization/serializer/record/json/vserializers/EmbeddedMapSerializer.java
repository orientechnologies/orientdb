package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.util.Map;

public class EmbeddedMapSerializer implements ValueSerializer {
    @Override
    public void toJSON(JsonGenerator generator, Object value) throws IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }


        final Map<?, ?> map = (Map<?, ?>) value;

        generator.writeStartObject();
        try {
            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                final String key = entry.getKey().toString();
                final Object item = entry.getValue();

                generator.writeFieldName(key);
                if (item == null) {
                    generator.writeNull();
                } else {
                    final OType type = OType.getTypeByValue(item);
                    final ValueSerializer serializer = SerializerFactory.INSTANCE.findSerializer(type);
                    serializer.toJSON(generator, item);
                }
            }

        } finally {
            generator.writeStartObject();
        }
    }

    @Override
    public String typeId() {
        return "p";
    }
}
