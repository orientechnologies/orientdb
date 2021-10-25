package com.orientechnologies.orient.core.serialization.serializer.record.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.IOException;

public interface ValueSerializer {
    void toJSON(JsonGenerator generator, Object value) throws IOException;
    Object fromJSON(JsonParser parser, ODocument owner) throws IOException;

    String typeId();

    JsonToken[] startTokens();
}
