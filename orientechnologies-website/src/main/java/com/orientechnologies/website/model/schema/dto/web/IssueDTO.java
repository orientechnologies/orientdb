package com.orientechnologies.website.model.schema.dto.web;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 13/11/14.
 */
public class IssueDTO {

  private String       title;
  private String       body;
  private String       state;
  private List<String> labels = new ArrayList<String>();
  private Integer      milestone;
  private Integer      version;
  private String       user;
  private String       assignee;
  private Boolean      confidential;

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public Integer getMilestone() {
    return milestone;
  }

  public void setMilestone(Integer milestone) {
    this.milestone = milestone;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getAssignee() {
    return assignee;
  }

  public void setAssignee(String assignee) {
    this.assignee = assignee;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public List<String> getLabels() {
    return labels;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  public Boolean getConfidential() {
    return confidential;
  }

  public void setConfidential(Boolean confidential) {
    this.confidential = confidential;
  }

}
