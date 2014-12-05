package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public class Issue {

  @JsonIgnore
  private String            id;
  private String            uuid;
  private Integer           number;
  private String            title;
  private String            body;
  private String            state;
  private Collection<Label> labels = new ArrayList<Label>();
  private Repository        repository;
  private Milestone         milestone;
  private Milestone         version;
  private Priority          priority;
  private Scope             scope;
  private Date              createdAt;
  private Date              updatedAt;
  private Date              closedAt;
  private Long              comments;
  private OUser             user;
  private OUser             assignee;
  private Boolean           confidential;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getNumber() {
    return number;
  }

  public Milestone getMilestone() {
    return milestone;
  }

  public void setMilestone(Milestone milestone) {
    this.milestone = milestone;
  }

  public Priority getPriority() {
    return priority;
  }

  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  public Scope getScope() {
    return scope;
  }

  public void setScope(Scope scope) {
    this.scope = scope;
  }

  public void setNumber(Integer number) {
    this.number = number;
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

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public Collection<Label> getLabels() {
    return labels;
  }

  public void setLabels(List<Label> labels) {
    this.labels = labels;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  public Date getClosedAt() {
    return closedAt;
  }

  public Date getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Date updatedAt) {
    this.updatedAt = updatedAt;
  }

  public void setClosedAt(Date closedAt) {
    this.closedAt = closedAt;
  }

  public OUser getAssignee() {
    return assignee;
  }

  public void setAssignee(OUser assignee) {
    this.assignee = assignee;
  }

  public void addLabel(Label name) {
    labels.add(name);
  }

  public OUser getUser() {
    return user;
  }

  public void setUser(OUser user) {
    this.user = user;
  }

  public Repository getRepository() {
    return repository;
  }

  public void setRepository(Repository repository) {
    this.repository = repository;
  }

  public Long getComments() {
    return comments;
  }

  public void setComments(Long comments) {
    this.comments = comments;
  }

  public Boolean getConfidential() {
    return confidential;
  }

  public void setConfidential(Boolean confidential) {
    this.confidential = confidential;
  }

  public Milestone getVersion() {
    return version;
  }

  public void setVersion(Milestone version) {
    this.version = version;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uid) {
    this.uuid = uid;
  }

  public enum IssueState {
    OPEN("OPEN"), CLOSED("CLOSED");
    private String state;

    IssueState(String state) {

      this.state = state;
    }

    @Override
    public String toString() {
      return state;
    }
  }
}
