package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.Collections;
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
    return String.join("->", indexes);
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

  @Override
  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    return Optional.empty();
  }

  @Override
  public List<String> properties() {
    return Collections.emptyList();
  }

  public boolean requiresDistinctStep(OCommandContext ctx) {
    // TODO: this should check all the chain of index for duplicate multi values,
    return true;
  }

  @Override
  public boolean fullySorted(List<String> properties, OCommandContext ctx) {
    // TODO  this should check all the chain of index for duplicate multi values,
    return false;
  }

  @Override
  public List<PropertyValue> values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PropertyValue> toValues() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDirectIndex() {
    return false;
  }
}
