package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class LinkBagSerializer extends AbstractLinkedCollectionSerializer
    implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    //noinspection unchecked
    writeLinkCollection(generator, (Iterable<OIdentifiable>) value);
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    if (owner == null) {
      throw new ODatabaseException("RidBags can be used only inside of documents.");
    }

    final ORidBag ridBag = new ORidBag();
    ridBag.setOwner(owner);

    readLinkCollection(parser, ridBag);
    return ridBag;
  }

  @Override
  public String typeId() {
    return SerializerIDs.LINK_BAG;
  }

  @Override
  public JsonToken[] startTokens() {
    return new JsonToken[] {JsonToken.START_ARRAY};
  }
}
