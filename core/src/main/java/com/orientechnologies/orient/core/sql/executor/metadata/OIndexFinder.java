package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Optional;

public interface OIndexFinder {

  public enum Operation {
    Eq,
    Gt,
    Lt,
    Ge,
    Le,
    FuzzyEq,
    Range;

    public boolean isRange() {
      return this == Gt || this == Lt || this == Ge || this == Le;
    }
  }

  Optional<OIndexCandidate> findExactIndex(OPath fieldName, Object value, OCommandContext ctx);

  Optional<OIndexCandidate> findByKeyIndex(OPath fieldName, Object value, OCommandContext ctx);

  Optional<OIndexCandidate> findAllowRangeIndex(
      OPath fieldName, Operation operation, Object value, OCommandContext ctx);

  Optional<OIndexCandidate> findByValueIndex(OPath fieldName, Object value, OCommandContext ctx);

  Optional<OIndexCandidate> findFullTextIndex(OPath fieldName, Object value, OCommandContext ctx);
}
