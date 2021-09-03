package com.orientechnologies.orient.core.metadata.security;

import java.util.Objects;

public class OSecurityResourceFunction extends OSecurityResource {

  public static final OSecurityResourceFunction ALL_FUNCTIONS =
      new OSecurityResourceFunction("database.function.*", "*");

  private final String functionName;

  public OSecurityResourceFunction(String resourceString, String functionName) {
    super(resourceString);
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OSecurityResourceFunction that = (OSecurityResourceFunction) o;
    return Objects.equals(functionName, that.functionName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionName);
  }
}
