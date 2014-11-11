package com.orientechnologies.website.model.schema;

/**
 * Created by Enrico Risa on 11/11/14.
 */
public enum HasOpened {
  ;
  private final String description;

  HasOpened(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
