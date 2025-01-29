package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 12/11/14. */
public interface OBinaryCompareOperator {
  public boolean execute(Object left, Object right, OCommandContext ctx);

  boolean supportsBasicCalculation();

  void toGenericStatement(StringBuilder builder);

  OBinaryCompareOperator copy();

  default boolean isRange() {
    return false;
  }

  OIndexFinder.Operation getOperation();

  boolean isInclude();

  boolean isLess();

  boolean isGreater();

  default boolean isGreaterInclude() {
    return isGreater() && isInclude();
  }

  default boolean isLessInclude() {
    return isLess() && isInclude();
  }

  default OIndexStream createStreamForIndex(
      OIndexInternal index, Object rightValue, boolean orderAsc, OCommandContext ctx) {
    throw new OCommandExecutionException("search for index for " + this + " is not supported yet");
  }

  default Optional<Map<Object, Object>> createIndexValueMap(Object object) {
    return Optional.empty();
  }
}
