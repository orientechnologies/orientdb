package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;

/**
 * Created by Enrico Risa on 28/05/15.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class Topic {

  @JsonIgnore
  private String       id;
  private String       uuid;
  private Long         number;
  private String       title;
  private String       body;
  private Organization organization;
  private Date         createdAt;
  private Date         updatedAt;
  private Long         comments;
  private OUser        user;
  private Boolean      confidential;

  public String getId() {

    return id;
  }

  public Long getNumber() {
    return number;
  }

  public void setNumber(Long number) {
    this.number = number;
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

  public Organization getOrganization() {
    return organization;
  }

  public void setOrganization(Organization organization) {
    this.organization = organization;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  public Date getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Date updatedAt) {
    this.updatedAt = updatedAt;
  }

  public Long getComments() {
    return comments;
  }

  public void setComments(Long comments) {
    this.comments = comments;
  }

  public OUser getUser() {
    return user;
  }

  public void setUser(OUser user) {
    this.user = user;
  }

  public Boolean getConfidential() {
    return confidential;
  }

  public void setConfidential(Boolean confidential) {
    this.confidential = confidential;
  }
}
