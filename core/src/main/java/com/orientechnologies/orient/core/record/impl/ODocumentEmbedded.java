package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;

public class ODocumentEmbedded extends ODocument {

  public ODocumentEmbedded() {
    super();
  }

  public ODocumentEmbedded(String clazz) {
    super(clazz);
  }

  public ODocumentEmbedded(String clazz, ODatabaseSession session) {
    super(clazz, session);
  }

  public ODocumentEmbedded(ODatabaseSession session) {
    super(session);
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }
}
