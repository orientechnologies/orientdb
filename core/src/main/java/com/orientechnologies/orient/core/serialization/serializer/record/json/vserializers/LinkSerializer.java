package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public class LinkSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    if (value == null) {
      generator.writeNull();
      return;
    }

    final OIdentifiable identifiable = (OIdentifiable) value;
    final ORID rid = identifiable.getIdentity();

    generator.writeString(rid.toString());
  }

  @Override
  public String typeId() {
    return SerializerIDs.LINK;
  }
}
