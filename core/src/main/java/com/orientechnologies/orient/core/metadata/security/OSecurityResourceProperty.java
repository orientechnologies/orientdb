package com.orientechnologies.orient.core.metadata.security;

import java.util.Objects;

public class OSecurityResourceProperty extends OSecurityResource {

  private final String className;
  private final boolean allClasses;
  private final String propertyName;

  public static final OSecurityResourceProperty ALL_PROPERTIES =
      new OSecurityResourceProperty("database.class.*.*", "*");

  public OSecurityResourceProperty(String resourceString, String className, String propertyName) {
    super(resourceString);
    this.className = className;
    allClasses = false;
    this.propertyName = propertyName;
  }

  public OSecurityResourceProperty(String resourceString, String propertyName) {
    super(resourceString);
    this.allClasses = true;
    this.className = null;
    this.propertyName = propertyName;
  }

  public String getClassName() {
    return className;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public boolean isAllClasses() {
    return allClasses;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OSecurityResourceProperty that = (OSecurityResourceProperty) o;
    return allClasses == that.allClasses
        && Objects.equals(className, that.className)
        && Objects.equals(propertyName, that.propertyName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, allClasses, propertyName);
  }
}
