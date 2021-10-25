package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class LinkListSerializer extends AbstractLinkedCollectionSerializer
    implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    //noinspection unchecked
    writeLinkCollection(generator, (Iterable<OIdentifiable>) value);
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    final ORecordLazyList list = new ORecordLazyList(owner);

    readLinkCollection(parser, list);

    return list;
  }

  @Override
  public String typeId() {
    return SerializerIDs.LINK_LIST;
  }

  @Override
  public JsonToken[] startTokens() {
    return new JsonToken[] {JsonToken.START_ARRAY};
  }
}
