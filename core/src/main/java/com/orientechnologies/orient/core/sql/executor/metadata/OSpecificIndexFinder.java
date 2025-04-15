package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import java.util.Collections;
import java.util.Optional;

public class OSpecificIndexFinder implements OIndexFinder {

  private OIndex index;

  public OSpecificIndexFinder(OIndex index) {
    this.index = index;
  }

  private boolean lastIsKey(OPath fieldName) {
    if (fieldName.getPath().isEmpty()) {
      return false;
    } else {
      return "key".equals(fieldName.getPath().get(fieldName.getPath().size() - 1));
    }
  }

  @Override
  public Optional<OIndexCandidate> findExact(
      OPath fieldName, OIndexKeySource value, OCommandContext ctx) {
    if (lastIsKey(fieldName)) {
      if (index.getInternal().canBeUsedInEqualityOperators()) {
        return Optional.of(
            new OIndexCandidateOne(index.getName(), Operation.Eq, fieldName(), value, false));
      }
    }
    return Optional.empty();
  }

  protected String fieldName() {
    return index.getDefinition().getFields().get(0);
  }

  @Override
  public Optional<OIndexCandidate> findNull(OPath fieldName, OCommandContext ctx) {
    if (lastIsKey(fieldName)) {
      if (index.getInternal().canBeUsedInEqualityOperators()) {
        return Optional.of(
            new OIndexCandidateOne(
                index.getName(),
                Operation.Eq,
                fieldName(),
                (c, asc) -> {
                  return Collections.singleton(null);
                },
                false));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByKey(
      OPath fieldName, OIndexKeySource value, OCommandContext ctx) {
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findAllowRange(
      OPath fieldName, Operation operation, OIndexKeySource value, OCommandContext ctx) {
    if (lastIsKey(fieldName)) {
      if (index.getInternal().canBeUsedInEqualityOperators()) {
        return Optional.of(
            new OIndexCandidateOne(
                index.getName(),
                operation,
                fieldName(),
                (c, asc) -> {
                  return Collections.singleton(null);
                },
                false));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findRange(
      OPath fieldName, OIndexKeySource first, OIndexKeySource second, OCommandContext ctx) {
    if (lastIsKey(fieldName)) {
      if (index.getInternal().canBeUsedInEqualityOperators()) {
        return Optional.of(
            new OIndexCandidateOne(
                index.getName(), fieldName(), Operation.Ge, first, Operation.Le, second, false));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByValue(
      OPath fieldName, OIndexKeySource value, OCommandContext ctx) {
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findFullText(
      OPath fieldName, OIndexKeySource value, OCommandContext ctx) {
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findAny(
      OPath oPath, OIndexKeySource value, OCommandContext ctx) {
    return Optional.empty();
  }
}
