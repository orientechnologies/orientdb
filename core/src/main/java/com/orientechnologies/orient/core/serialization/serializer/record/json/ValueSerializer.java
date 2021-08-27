package com.orientechnologies.orient.core.serialization.serializer.record.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public interface ValueSerializer {
    void toJSON(JsonGenerator generator, Object value) throws IOException;
    String typeId();
}
