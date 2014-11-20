package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 07/11/14.
 */
public class IssueEvent extends Event {

  private Integer   eventId;
  private String    event;
  private OUser actor;
  private String    commitId;
  private Label     label;
  private OUser assignee;
  private Milestone milestone;

  public void setEventId(Integer eventId) {
    this.eventId = eventId;
  }

  public Integer getEventId() {
    return eventId;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public OUser getActor() {
    return actor;
  }

  public void setActor(OUser actor) {
    this.actor = actor;
  }

  public String getCommitId() {
    return commitId;
  }

  public void setCommitId(String commitId) {
    this.commitId = commitId;
  }

  public Label getLabel() {
    return label;
  }

  public void setLabel(Label label) {
    if (label != null)
      label.setId(null);
    this.label = label;
  }

  public OUser getAssignee() {
    return assignee;
  }

  public void setAssignee(OUser assignee) {
    this.assignee = assignee;
  }

  public Milestone getMilestone() {
    return milestone;
  }

  public void setMilestone(Milestone milestone) {
    if (milestone != null)
      milestone.setId(null);
    this.milestone = milestone;
  }
}
