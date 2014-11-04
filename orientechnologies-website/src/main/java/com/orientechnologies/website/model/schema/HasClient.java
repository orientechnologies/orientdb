package com.orientechnologies.website.model.schema;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum HasClient {
  ;
  private final String description;

  HasClient(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
