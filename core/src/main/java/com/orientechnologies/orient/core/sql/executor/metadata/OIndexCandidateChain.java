package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OIndexCandidateChain implements OIndexCandidate {

  private List<String> indexes = new ArrayList<>();
  private Operation operation;

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

  public void add(String name) {
    indexes.add(name);
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public Operation getOperation() {
    return operation;
  }
}
