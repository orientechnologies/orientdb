package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

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

  public OImmutableSecurityPolicy(OSecurityPolicy element) {
    this.identity = element.getIdentity();
    this.name = element.getName();
    this.active = element.isActive();
    this.create = element.getCreateRule();
    this.read = element.getReadRule();
    this.beforeUpdate = element.getBeforeUpdateRule();
    this.afterUpdate = element.getAfterUpdateRule();
    this.delete = element.getDeleteRule();
    this.execute = element.getExecuteRule();
  }

  public OImmutableSecurityPolicy(
      String name,
      String create,
      String read,
      String beforeUpdate,
      String afterUpdate,
      String delete,
      String execute) {
    super();
    this.identity = new ORecordId(-1, -1);
    this.active = true;
    this.name = name;
    this.create = create;
    this.read = read;
    this.beforeUpdate = beforeUpdate;
    this.afterUpdate = afterUpdate;
    this.delete = delete;
    this.execute = execute;
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
