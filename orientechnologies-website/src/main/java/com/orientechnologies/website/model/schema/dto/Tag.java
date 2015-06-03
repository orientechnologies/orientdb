package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.orientechnologies.website.model.schema.Identity;

import javax.persistence.Entity;

/**
 * Created by Enrico Risa on 03/06/15.
 */
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tag extends Identity {

  private String name;
  private String color;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getColor() {
    return color;
  }

  public void setColor(String color) {
    this.color = color;
  }
}
