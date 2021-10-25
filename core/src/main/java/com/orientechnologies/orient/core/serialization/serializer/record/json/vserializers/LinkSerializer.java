package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class LinkSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    final OIdentifiable identifiable = (OIdentifiable) value;
    final ORID rid = identifiable.getIdentity();

    generator.writeString(rid.toString());
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    return new ORecordId(parser.getValueAsString());
  }

  @Override
  public String typeId() {
    return SerializerIDs.LINK;
  }

  @Override
  public JsonToken[] startTokens() {
    return new JsonToken[] {JsonToken.VALUE_STRING};
  }
}
