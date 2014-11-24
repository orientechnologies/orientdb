package com.orientechnologies.website.model.schema.dto.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

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
  private Integer      priority;
  private String       assignee;
  private Integer      scope;
  private Boolean      confidential;

  @JsonIgnore
  public String getState() {
    return state;
  }

  @JsonProperty
  public void setState(String state) {
    this.state = state;
  }

  public Integer getMilestone() {
    return milestone;
  }

  public void setMilestone(Integer milestone) {
    this.milestone = milestone;
  }

  @JsonIgnore
  public Integer getVersion() {
    return version;
  }

  @JsonProperty
  public void setVersion(Integer version) {
    this.version = version;
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

  @JsonIgnore
  public Boolean getConfidential() {
    return confidential;
  }

  @JsonProperty
  public void setConfidential(Boolean confidential) {
    this.confidential = confidential;
  }

  public Integer getPriority() {
    return priority;
  }

  public void setPriority(Integer priority) {
    this.priority = priority;
  }

  public Integer getScope() {
    return scope;
  }

  public void setScope(Integer scope) {
    this.scope = scope;
  }
}
