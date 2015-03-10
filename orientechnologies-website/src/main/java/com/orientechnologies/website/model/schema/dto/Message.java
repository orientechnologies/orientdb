package com.orientechnologies.website.model.schema.dto;

import java.util.Date;

/**
 * Created by Enrico Risa on 19/01/15.
 */
public class Message {

  protected String  id;
  protected String  uuid;
  protected String  body;
  protected Date    date;
  protected Integer clientId;
  protected OUser   sender;
  protected Boolean edited;
  
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

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public OUser getSender() {
    return sender;
  }

  public void setSender(OUser sender) {
    this.sender = sender;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getClientId() {
    return clientId;
  }

  public void setClientId(Integer clientId) {
    this.clientId = clientId;
  }

  public Boolean getEdited() {
    return edited;
  }

  public void setEdited(Boolean edited) {
    this.edited = edited;
  }
}
