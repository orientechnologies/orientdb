package com.orientechnologies.website.model.schema;

/**
* Created by Enrico Risa on 04/11/14.
*/
public enum HasBot {
  ;
  private final String description;

  HasBot(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
