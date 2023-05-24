package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Optional;

public class OIndexCandidateImpl implements OIndexCandidate {

  private String name;
  private Operation operation;

  public OIndexCandidateImpl(String name, Operation operation) {
    this.name = name;
    this.operation = operation;
  }

  public String getName() {
    return name;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    if (this.operation == Operation.Ge) {
      this.operation = Operation.Lt;
    } else if (this.operation == Operation.Gt) {
      this.operation = Operation.Le;
    } else if (this.operation == Operation.Le) {
      this.operation = Operation.Gt;
    } else if (this.operation == Operation.Lt) {
      this.operation = Operation.Ge;
    }
    return Optional.of(this);
  }

  public Operation getOperation() {
    return operation;
  }
}
