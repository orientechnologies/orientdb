package com.orientechnologies.website.model.schema.dto;

import java.util.List;

/**
 * Created by Enrico Risa on 04/12/14.
 */
public class Environment {

  protected String    id;
  protected String    name;
  protected String    description;

  protected List<Sla> slaConfigurations;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<Sla> getSlaConfigurations() {
    return slaConfigurations;
  }

  public void setSlaConfigurations(List<Sla> slaConfigurations) {
    this.slaConfigurations = slaConfigurations;
  }
}
