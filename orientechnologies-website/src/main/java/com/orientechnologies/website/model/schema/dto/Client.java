package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public class Client {

  protected String  name;
  protected String  id;
  protected Integer clientId;

  public Integer getClientId() {
    return clientId;
  }

  public void setClientId(Integer clientId) {
    this.clientId = clientId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

}
