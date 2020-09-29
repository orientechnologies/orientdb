package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;

public class OImmutableSecurityPolicy implements OSecurityPolicy {

  private final ORID identity;
  private final String name;
  private final boolean active;
  private final String create;
  private final String read;
  private final String beforeUpdate;
  private final String afterUpdate;
  private final String delete;
  private final String execute;

  public OImmutableSecurityPolicy(OElement element) {
    this.identity = element.getIdentity();
    this.name = element.getProperty("name");
    this.active = element.hasProperty("active") ? element.getProperty("active") : false;
    this.create = element.getProperty("create");
    this.read = element.getProperty("read");
    this.beforeUpdate = element.getProperty("beforeUpdate");
    this.afterUpdate = element.getProperty("afterUpdate");
    this.delete = element.getProperty("delete");
    this.execute = element.getProperty("execute");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public String getCreateRule() {
    return create;
  }

  @Override
  public String getReadRule() {
    return read;
  }

  @Override
  public String getBeforeUpdateRule() {
    return beforeUpdate;
  }

  @Override
  public String getAfterUpdateRule() {
    return afterUpdate;
  }

  @Override
  public String getDeleteRule() {
    return delete;
  }

  @Override
  public String getExecuteRule() {
    return execute;
  }

  @Override
  public ORID getIdentity() {
    return identity;
  }
}
