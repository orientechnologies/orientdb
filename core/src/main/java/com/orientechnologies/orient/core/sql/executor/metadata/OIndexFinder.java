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

  Optional<OIndexCandidate> findExact(OPath fieldName, OIndexKeySource value, OCommandContext ctx);

  Optional<OIndexCandidate> findNull(OPath fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findByKey(OPath fieldName, OIndexKeySource value, OCommandContext ctx);

  Optional<OIndexCandidate> findAllowRange(
      OPath fieldName, Operation operation, OIndexKeySource value, OCommandContext ctx);

  Optional<OIndexCandidate> findRange(
      OPath fieldName, OIndexKeySource first, OIndexKeySource second, OCommandContext ctx);

  Optional<OIndexCandidate> findByValue(
      OPath fieldName, OIndexKeySource value, OCommandContext ctx);

  Optional<OIndexCandidate> findFullText(
      OPath fieldName, OIndexKeySource value, OCommandContext ctx);
}
