package com.orientechnologies.orient.core.sql.functions.math;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import java.util.List;

/**
 * Returns the index of the argument that is more than the first argument.
 *
 * <p>Returns -1 if 1st number is NULL or if 1st number is the highest number The returned index
 * starts from the 2nd argument. That is, interval(23, 5, 50) = 1
 *
 * <p>Few examples: interval(43, 35, 5, 15, 50) = 3 interval(54, 25, 35, 45) = -1 interval(null, 5,
 * 50) = -1 interval(6, 6) = -1 interval(58, 60, 30, 65) = 0 interval(103, 54, 106, 98, 119) = 1
 *
 * @author Matan Shukry (matanshukry@gmail.com)
 */
public class OSQLFunctionInterval extends OSQLFunctionMathAbstract {
  public static final String NAME = "interval";

  private OSQLPredicate predicate;

  public OSQLFunctionInterval() {
    super(NAME, 2, 0);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Object execute(
      Object iThis,
      final OIdentifiable iRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {

    if (iParams == null || iParams.length < 2) {
      return -1;
    }

    // return the index of the number the 1st number
    final Comparable first = (Comparable<?>) iParams[0];
    if (first == null) {
      return -1;
    }

    for (int i = 1; i < iParams.length; ++i) {
      final Comparable other = (Comparable<?>) iParams[i];
      if (other.compareTo(first) > 0) {
        return i - 1;
      }
    }

    return -1;
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "interval(<field> [,<field>*])";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    return null;
  }
}
