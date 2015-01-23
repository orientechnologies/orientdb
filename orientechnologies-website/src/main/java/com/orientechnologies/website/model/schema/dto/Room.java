package com.orientechnologies.website.model.schema.dto;

import java.util.Date;

/**
 * Created by Enrico Risa on 22/01/15.
 */
public class Room extends Client {

  protected Date timestamp;

  protected Date lastVisit;

  public Room(Client client) {
    clientId = client.clientId;
    name = client.name;
    id = client.id;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  public Date getLastVisit() {
    return lastVisit;
  }

  public void setLastVisit(Date lastVisit) {
    this.lastVisit = lastVisit;
  }
}
