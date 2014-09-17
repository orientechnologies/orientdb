package com.orientechnologies.orient.core.record.impl;

public class ODocumentInternal {

  public static void clearSource(ODocument document) {
    document.clearSource();
  }

  public static void convertAllMultiValuesToTrackedVersions(ODocument document) {
    document.convertAllMultiValuesToTrackedVersions();
  }

}
