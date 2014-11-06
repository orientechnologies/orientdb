package com.orientechnologies.website.github;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public enum GIssueState {
  OPEN("open"), CLOSED("closed");
  private String name;

  GIssueState(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }

}
