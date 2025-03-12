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

    boolean isInclude() {
      return this == Ge || this == Le;
    }

    boolean isL() {
      return this == Lt || this == Le;
    }

    boolean isG() {
      return this == Gt || this == Ge;
    }

    boolean canRangeWith(Operation other) {
      if (this.isRange() && other.isRange()) {
        if (this.isL()) {
          return other.isG();
        } else {
          return other.isL();
        }
      } else {
        return false;
      }
    }
  }

  Optional<OIndexCandidate> findExact(OPath fieldName, Object value, OCommandContext ctx);

  Optional<OIndexCandidate> findNull(OPath fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findByKey(OPath fieldName, Object value, OCommandContext ctx);

  Optional<OIndexCandidate> findAllowRange(
      OPath fieldName, Operation operation, Object value, OCommandContext ctx);

  Optional<OIndexCandidate> findRange(
      OPath fieldName, Object first, Object second, OCommandContext ctx);

  Optional<OIndexCandidate> findByValue(OPath fieldName, Object value, OCommandContext ctx);

  Optional<OIndexCandidate> findFullText(OPath fieldName, Object value, OCommandContext ctx);
}
