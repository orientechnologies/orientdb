package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Date;

/**
 * Created by Enrico Risa on 28/05/15.
 */
public class TopicComment {

  @JsonIgnore
  private String  id;
  private String  uuid;
  private String  body;
  private Date    createdAt;
  private Date    updatedAt;
  private OUser   user;
  private boolean confidential;

  public Date getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Date updatedAt) {
    this.updatedAt = updatedAt;
  }

  public Date getCreatedAt() {

    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public OUser getUser() {
    return user;
  }

  public void setUser(OUser user) {
    this.user = user;
  }

  public boolean isConfidential() {
    return confidential;
  }

  public void setConfidential(boolean confidential) {
    this.confidential = confidential;
  }
}
