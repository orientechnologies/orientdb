package com.orientechnologies.website.github;

import java.util.Date;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GMilestone extends GEntity {
  protected GMilestone(GitHub github, GEntity owner, String content) {
    super(github, owner, content);
  }

  public Integer getNumber() {
    return get("number");
  }

  public String getTitle() {
    return get("title");
  }

  public String getDescription() {
    return get("description");
  }

  public GIssueState getState() {
    return GIssueState.valueOf(((String) get("state")).toUpperCase());
  }

  public Date getCreatedAt() {
    return toDate((String) get("created_at"));
  }

  public Date getDueOn() {
    return toDate((String) get("due_on"));
  }

  @Override
  protected String getBaseUrl() {
    return null;
  }
}
