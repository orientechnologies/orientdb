package com.orientechnologies.website.model.schema;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum HasScope {
  ;
  private final String description;

  HasScope(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
