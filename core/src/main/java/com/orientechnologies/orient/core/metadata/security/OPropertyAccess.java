package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Set;

public class OPropertyAccess {

  private Set<String> filtered;

  public OPropertyAccess(ODocument document, OSecurityInternal security) {
    filtered = security.getFilteredProperties(document);
  }

  public OPropertyAccess(Set<String> filtered) {
    this.filtered = filtered;
  }

  public boolean isReadable(String property) {
    return filtered == null || !filtered.contains(property);
  }

}
