package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;

public class OSecurityPolicy {

  public enum Scope {
    CREATE, READ, BEFORE_UPDATE, AFTER_UPDATE, DELETE, EXECUTE
  }

  private OElement element;

  public OSecurityPolicy(OElement element) {
    this.element = element;
  }

  public OElement getElement() {
    return element;
  }

  public void setElement(OElement element) {
    this.element = element;
  }

  public String getName() {
    return element.getProperty("name");
  }

  public void setName(String name) {
    element.setProperty("name", name);
  }

  public boolean isActive() {
    return Boolean.TRUE.equals(this.element.getProperty("active"));
  }

  public void setActive(Boolean active) {
    this.element.setProperty("active", active);
  }

  public String getCreateRule() {
    return element == null ? null : element.getProperty("create");
  }

  public void setCreateRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    element.setProperty("create", rule);
  }

  public String getReadRule() {
    return element == null ? null : element.getProperty("read");
  }

  public void setReadRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    element.setProperty("read", rule);
  }

  public String getBeforeUpdateRule() {
    return element == null ? null : element.getProperty("beforeUpdate");
  }

  public void setBeforeUpdateRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    element.setProperty("beforeUpdate", rule);
  }

  public String getAfterUpdateRule() {
    return element == null ? null : element.getProperty("afterUpdate");
  }

  public void setAfterUpdateRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    element.setProperty("afterUpdate", rule);
  }

  public String getDeleteRule() {
    return element == null ? null : element.getProperty("delete");
  }

  public void setDeleteRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    element.setProperty("delete", rule);
  }

  public String getExecuteRule() {
    return element == null ? null : element.getProperty("execute");
  }

  public void setExecuteRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    element.setProperty("execute", rule);
  }

  public String get(Scope scope) {
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

  protected void validatePredicate(String predicate) throws IllegalArgumentException {
    if (predicate == null || predicate.trim().length() == 0) {
      return;
    }
    try {
      OSQLEngine.parsePredicate(predicate);
    } catch (OCommandSQLParsingException ex) {
      throw new IllegalArgumentException("Invalid predicate: " + predicate);
    }
  }
}
