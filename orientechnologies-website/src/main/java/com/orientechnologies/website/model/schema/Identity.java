package com.orientechnologies.website.model.schema;

/**
 * Created by Enrico Risa on 03/06/15.
 */
public abstract class Identity {

  private String id;
  private String uuid;

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
}
