package com.orientechnologies.orient.core.metadata.security;

public class OSecurityResourceSchema extends OSecurityResource {

  public static OSecurityResourceSchema INSTANCE = new OSecurityResourceSchema("database.schema");

  private OSecurityResourceSchema(String resourceString) {
    super(resourceString);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof OSecurityResourceSchema;
  }

  @Override
  public int hashCode() {
    return 1;
  }
}
