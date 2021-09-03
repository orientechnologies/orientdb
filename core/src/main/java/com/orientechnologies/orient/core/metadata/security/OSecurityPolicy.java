package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.id.ORID;

public interface OSecurityPolicy {

  public enum Scope {
    CREATE,
    READ,
    BEFORE_UPDATE,
    AFTER_UPDATE,
    DELETE,
    EXECUTE
  }

  ORID getIdentity();

  String getName();

  boolean isActive();

  String getCreateRule();

  String getReadRule();

  String getBeforeUpdateRule();

  String getAfterUpdateRule();

  String getDeleteRule();

  String getExecuteRule();

  default String get(Scope scope) {
    switch (scope) {
      case CREATE:
        return getCreateRule();
      case READ:
        return getReadRule();
      case BEFORE_UPDATE:
        return getBeforeUpdateRule();
      case AFTER_UPDATE:
        return getAfterUpdateRule();
      case DELETE:
        return getDeleteRule();
      case EXECUTE:
        return getExecuteRule();
      default:
        throw new IllegalArgumentException();
    }
  }
}
