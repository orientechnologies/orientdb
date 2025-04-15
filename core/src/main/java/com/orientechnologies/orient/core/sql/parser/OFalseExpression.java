package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class OFalseExpression extends OBooleanExpression {
  public OFalseExpression(int id) {
    super(id);
  }

  @Override
  public boolean evaluate(OResult currentRecord, OCommandContext ctx) {
    return false;
  }

  @Override
  protected boolean supportsBasicCalculation() {
    return true;
  }

  @Override
  protected int getNumberOfExternalCalculations() {
    return 0;
  }

  @Override
  protected List<Object> getExternalCalculationConditions() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public boolean needsAliases(Set<String> aliases) {
    return false;
  }

  @Override
  public OBooleanExpression copy() {
    return FALSE;
  }

  @Override
  public List<String> getMatchPatternInvolvedAliases() {
    return null;
  }

  @Override
  public void translateLuceneOperator() {}

  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public String toString() {
    return "false";
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("false");
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append(PARAMETER_PLACEHOLDER);
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public void extractSubQueries(SubQueryCollector collector) {}

  @Override
  public boolean refersToParent() {
    return false;
  }
}
