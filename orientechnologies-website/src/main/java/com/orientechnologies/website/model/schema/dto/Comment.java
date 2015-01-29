package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Date;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public class Comment extends Event {

  private String      id;
  private String      uuid;
  private Integer     commentId;
  private String      body;
  private OUser       user;

  @JsonIgnore
  protected ODocument internal;
  private Date        updatedAt;

  public Integer getCommentId() {
    return commentId;
  }

  public void setCommentId(Integer commentId) {
    this.commentId = commentId;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public OUser getUser() {
    return user;
  }

  public void setUser(OUser user) {
    this.user = user;
  }

  public Date getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Date updatedAt) {
    this.updatedAt = updatedAt;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getUuid() {
    return uuid;
  }

  public void setInternal(ODocument internal) {
    this.internal = internal;
  }

  public ODocument getInternal() {
    return internal;
  }
}
