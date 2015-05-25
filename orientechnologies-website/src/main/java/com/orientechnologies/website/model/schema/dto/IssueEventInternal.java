package com.orientechnologies.website.model.schema.dto;

import java.util.Date;

/**
 * Created by Enrico Risa on 21/11/14.
 */

public class IssueEventInternal extends IssueEvent {

  protected Milestone   version;
  protected Priority    priority;
  protected Scope       scope;
  protected Environment environment;

  protected Date        time;
  protected Boolean     secret;

  public Milestone getVersion() {
    return version;
  }

  public void setVersion(Milestone version) {
    if (version != null)
      version.setId(null);
    this.version = version;
  }

  public Priority getPriority() {
    return priority;
  }

  public void setPriority(Priority priority) {
    if (priority != null) {
      priority.setId(null);
    }
    this.priority = priority;
  }

  public Environment getEnvironment() {
    return environment;
  }

  public void setEnvironment(Environment environment) {
    if (environment != null) {
      environment.setId(null);
    }
    this.environment = environment;
  }

  public Scope getScope() {
    return scope;
  }

  public void setScope(Scope scope) {
    if (scope != null) {
      scope.setId(null);
    }
    this.scope = scope;
  }

  public Date getTime() {
    return time;
  }

  public void setTime(Date time) {
    this.time = time;
  }

  public void setSecret(Boolean secret) {
    this.secret = secret;
  }

  public Boolean getSecret() {
    return secret;
  }
}
