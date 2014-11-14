package com.orientechnologies.website.model.schema.dto;

/**
 * Created by enricorisa on 16/10/14.
 */
public class Repository {

  private String       id;
  private String       name;
  private String       description;
  private Organization organization;

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

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Organization getOrganization() {
    return organization;
  }

  public void setOrganization(Organization organization) {
    this.organization = organization;
  }
}
