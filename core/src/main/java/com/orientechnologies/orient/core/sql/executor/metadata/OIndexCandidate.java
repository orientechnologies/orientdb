package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Optional;

public interface OIndexCandidate {
  public String getName();

  public Optional<OIndexCandidate> invert();

  public Operation getOperation();
}
