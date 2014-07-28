package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.record.impl.ODocument;

public interface ODocumentSerializer {

  public void serialize(ODocument document, BytesContainer bytes);

  public void deserialize(ODocument document, BytesContainer bytes);

}
