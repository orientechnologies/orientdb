package com.orientechnologies.website.model.schema;

/**
* Created by Enrico Risa on 04/11/14.
*/
public enum HasVersion {
;
private final String description;

HasVersion(String description) {
  this.description = description;
}

@Override
public String toString() {
  return description;
}
}
