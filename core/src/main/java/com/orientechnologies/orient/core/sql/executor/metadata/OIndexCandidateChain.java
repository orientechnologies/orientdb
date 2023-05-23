package com.orientechnologies.orient.core.sql.executor.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OIndexCandidateChain implements OIndexCandidate {

  private List<String> indexes = new ArrayList<>();

  public OIndexCandidateChain(String name) {
    indexes.add(name);
  }

  @Override
  public String getName() {
    String name = "";
    for (String index : indexes) {
      name += index + "->";
    }
    return name;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    return Optional.of(this);
  }

  public void add(String name) {
    indexes.add(name);
  }
}
