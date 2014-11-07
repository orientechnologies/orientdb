package com.orientechnologies.website.model.schema;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum HasEvent {
  ;
  private final String description;

  HasEvent(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
