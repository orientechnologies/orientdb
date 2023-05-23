package com.orientechnologies.orient.core.sql.executor.metadata;

import java.util.Optional;

public class OIndexCandidateImpl implements OIndexCandidate {

  private String name;

  public OIndexCandidateImpl(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    // TODO: when handling operator invert it
    return Optional.of(this);
  }
}
