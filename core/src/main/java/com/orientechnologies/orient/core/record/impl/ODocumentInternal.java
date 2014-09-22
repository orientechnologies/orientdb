package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.ORecordElement;

public class ODocumentInternal {

  public static void clearSource(ODocument document) {
    document.clearSource();
  }

  public static void convertAllMultiValuesToTrackedVersions(ODocument document) {
    document.convertAllMultiValuesToTrackedVersions();
  }

  public static void addOwner(ODocument oDocument, ORecordElement iOwner) {
    oDocument.addOwner(iOwner);
  }

  public static void removeOwner(ODocument oDocument, ORecordElement iOwner) {
    oDocument.removeOwner(iOwner);
  }

}
