package com.orientechnologies.website.model.schema;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum HasEnvironment {
  ;
  private final String description;

  HasEnvironment(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
