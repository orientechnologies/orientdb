package com.orientechnologies.website.model.schema;

/**
 * Created by Enrico Risa on 11/11/14.
 */
public enum IsAssigned {
  ;
  private final String description;

  IsAssigned(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
