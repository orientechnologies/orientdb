package com.orientechnologies.orient.core.metadata.security;

public class OSecurityResourceAll extends OSecurityResource {

  public static OSecurityResourceAll INSTANCE = new OSecurityResourceAll("*");

  private OSecurityResourceAll(String resourceString) {
    super(resourceString);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof OSecurityResourceAll;
  }

  @Override
  public int hashCode() {
    return 1;
  }
}
