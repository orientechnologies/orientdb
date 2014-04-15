package com.orientechnologies.lucene.operator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;

/**
 * Created by enricorisa on 28/03/14.
 */
public class OLuceneOperatorFactory implements OQueryOperatorFactory {

  private static final Set<OQueryOperator> OPERATORS;

  static {
    final Set<OQueryOperator> operators = new HashSet<OQueryOperator>();
    operators.add(new OLuceneNearOperator());
    operators.add(new OLuceneWithinOperator());
    operators.add(new OLuceneTextOperator());
    OPERATORS = Collections.unmodifiableSet(operators);
  }

  @Override
  public Set<OQueryOperator> getOperators() {
    return OPERATORS;
  }
}
