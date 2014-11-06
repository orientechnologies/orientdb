package com.orientechnologies.website.model.schema.dto;

import java.util.Date;

/**
 * Created by Enrico Risa on 06/11/14.
 */
public class Event {

  private String  id;
  private Integer eventId;
  private Date    createdAt;

  public Date getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;

  }

  public void setEventId(Integer eventId) {
    this.eventId = eventId;
  }

  public Integer getEventId() {
    return eventId;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
