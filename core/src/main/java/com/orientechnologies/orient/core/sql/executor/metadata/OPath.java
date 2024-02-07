package com.orientechnologies.orient.core.sql.executor.metadata;

import java.util.ArrayList;
import java.util.List;

public class OPath {
  private List<String> path = new ArrayList<>();

  public OPath(String value) {
    this.path.add(value);
  }

  public void addPre(String value) {
    this.path.add(0, value);
  }

  public List<String> getPath() {
    return path;
  }
}
