package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * Created by luigidellaquila on 01/07/15.
 */
public abstract class OIdentity extends ODocumentWrapper {
  public final static String CLASS_NAME = "OIdentity";

  public OIdentity() {
  }

  public OIdentity(ORID iRID) {
    super(iRID);
  }

  public OIdentity(String iClassName) {
    super(iClassName);
  }

  public OIdentity(ODocument iDocument) {
    super(iDocument);
  }
}
