package com.orientechnologies.orient.test.domain.customserialization;

public enum SecurityRole {
  ADMIN("administrador"),
  LOGIN("login");

  private String id;

  private SecurityRole(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return id;
  }

  public static SecurityRole getByName(String name) {

    if (ADMIN.name().equals(name)) {
      return ADMIN;
    } else if (LOGIN.name().equals(name)) {
      return LOGIN;
    }

    return null;
  }

  public static SecurityRole[] toArray() {
    return new SecurityRole[] {ADMIN, LOGIN};
  }
}
