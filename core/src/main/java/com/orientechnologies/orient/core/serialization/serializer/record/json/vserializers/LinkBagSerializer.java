package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class LinkBagSerializer extends AbstractLinkedCollectionSerializer implements ValueSerializer {
    @Override
    public void toJSON(JsonGenerator generator, Object value) throws IOException {
        //noinspection unchecked
        writeLinkCollection(generator, (Iterable<OIdentifiable>) value);
    }

    @Override
    public String typeId() {
        return "g";
    }
}
