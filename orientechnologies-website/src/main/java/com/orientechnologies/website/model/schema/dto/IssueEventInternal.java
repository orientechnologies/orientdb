package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 21/11/14.
 */
public class IssueEventInternal extends IssueEvent {

  protected Milestone version;
  protected Priority  priority;

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
}
