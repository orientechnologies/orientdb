package com.orientechnologies.website.github;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Date;

/**
 * Created by Enrico Risa on 06/11/14.
 */
public class GEvent extends GEntity {

  protected GEvent(GitHub github, GEntity owner, String content) {
    super(github, owner, content);
  }

  public Integer getId() {
    return get("id");
  }

  public Date getCreatedAt() {
    return toDate((String) get("created_at"));
  }

  public GLabel getLabel() {

    ODocument doc = get("label");
    return doc != null ? new GLabel(github, this, doc.toJSON()) : null;
  }

  public GMilestone getMilestone() {

    ODocument doc = get("milestone");
    return doc != null ? new GMilestone(github, this, doc.toJSON()) : null;
  }

  @Override
  protected String getBaseUrl() {
    return null;
  }

  public String getEvent() {
    return get("event");
  }

  public GUser getActor() {
    return toUser((ODocument) get("actor"));
  }

  public GUser getAssignee() {
    return toUser((ODocument) get("assignee"));
  }
}
