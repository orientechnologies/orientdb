package com.orientechnologies.website.model.schema.dto;

import java.util.Date;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public class Milestone {

  private String  id;
  private Integer number;
  private String  state;
  private String  title;
  private String  description;
  private OUser creator;
  private Date    createdAt;
  private Date    updatedAt;


    public String getId() {
        return id;
    }

    public void setId(String id) {
    this.id = id;
    }

    public Date getDueOn() {
    return dueOn;
  }

  public void setDueOn(Date dueOn) {
    this.dueOn = dueOn;
  }

  public Integer getNumber() {
    return number;
  }

  public void setNumber(Integer number) {
    this.number = number;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public OUser getCreator() {
    return creator;
  }

  public void setCreator(OUser creator) {
    this.creator = creator;
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

  private Date dueOn;
}
