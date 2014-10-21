package com.orientechnologies.website.model.schema.dto;

/**
 * Created by enricorisa on 16/10/14.
 */
public class Repository {

  private String id;
  private String name;
  private String description;
  private String codename;

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCodename() {
    return codename;
  }

  public void setCodename(String codename) {
    this.codename = codename;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
