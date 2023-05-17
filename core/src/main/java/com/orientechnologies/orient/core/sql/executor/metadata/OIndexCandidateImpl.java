package com.orientechnologies.orient.core.sql.executor.metadata;

public class OIndexCandidateImpl implements OIndexCandidate {

  private String name;

  public OIndexCandidateImpl(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
