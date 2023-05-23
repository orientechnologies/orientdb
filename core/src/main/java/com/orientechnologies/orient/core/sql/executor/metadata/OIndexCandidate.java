package com.orientechnologies.orient.core.sql.executor.metadata;

import java.util.Optional;

public interface OIndexCandidate {
  public String getName();

  public Optional<OIndexCandidate> invert();
}
