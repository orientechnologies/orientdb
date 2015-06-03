package com.orientechnologies.website.model.schema.dto;

import com.orientechnologies.website.model.schema.Identity;

/**
 * Created by enricorisa on 16/10/14.
 */
public class Organization extends Identity {

  private String name;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
