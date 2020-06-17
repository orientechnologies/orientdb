package com.orientechnologies.orient.core.metadata.security;

import java.util.Objects;

public class OSecurityResourceClass extends OSecurityResource {

  public static final OSecurityResourceClass ALL_CLASSES =
      new OSecurityResourceClass("database.class.*", "*");

  private final String className;

  public OSecurityResourceClass(String resourceString, String className) {
    super(resourceString);
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OSecurityResourceClass that = (OSecurityResourceClass) o;
    return Objects.equals(className, that.className);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className);
  }
}
