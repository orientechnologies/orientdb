package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class EmbeddedListSerializer extends AbstractEmbeddedCollectionSerializer
    implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    writeEmbeddedCollection(generator, (Iterable<?>) value);
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    final Collection<Object> collection;
    if (owner != null) {
      collection = new OTrackedList<>(owner);
    } else {
      collection = new ArrayList<>();
    }
    readEmbeddedCollection(parser, collection);

    return collection;
  }

  @Override
  public String typeId() {
    return SerializerIDs.EMBEDDED_LIST;
  }
}
