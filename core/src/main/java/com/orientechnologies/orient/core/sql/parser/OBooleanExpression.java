package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.OIndexSearchInfo;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Created by luigidellaquila on 07/11/14. */
public abstract class OBooleanExpression extends SimpleNode {

  public static final OBooleanExpression TRUE =
      new OBooleanExpression(0) {
        @Override
        public boolean evaluate(OIdentifiable currentRecord, OCommandContext ctx) {
          return true;
        }

        @Override
        public boolean evaluate(OResult currentRecord, OCommandContext ctx) {
          return true;
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
          return TRUE;
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
          return "true";
        }

        public void toString(Map<Object, Object> params, StringBuilder builder) {
          builder.append("true");
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

        @Override
        public boolean isAlwaysTrue() {
          return true;
        }
      };

  public static final OBooleanExpression FALSE =
      new OBooleanExpression(0) {
        @Override
        public boolean evaluate(OIdentifiable currentRecord, OCommandContext ctx) {
          return false;
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
      };

  public OBooleanExpression(int id) {
    super(id);
  }

  public OBooleanExpression(OrientSql p, int id) {
    super(p, id);
  }

  public abstract boolean evaluate(OIdentifiable currentRecord, OCommandContext ctx);

  public abstract boolean evaluate(OResult currentRecord, OCommandContext ctx);

  /**
   * @return true if this expression can be calculated in plain Java, false otherwise (eg. LUCENE
   *     operator)
   */
  protected abstract boolean supportsBasicCalculation();

  /**
   * @return the number of sub-expressions that have to be calculated using an external engine (eg.
   *     LUCENE)
   */
  protected abstract int getNumberOfExternalCalculations();

  /**
   * @return the sub-expressions that have to be calculated using an external engine (eg. LUCENE)
   */
  protected abstract List<Object> getExternalCalculationConditions();

  public List<OBinaryCondition> getIndexedFunctionConditions(
      OClass iSchemaClass, ODatabaseDocumentInternal database) {
    return null;
  }

  public List<OAndBlock> flatten() {

    return Collections.singletonList(encapsulateInAndBlock(this));
  }

  protected OAndBlock encapsulateInAndBlock(OBooleanExpression item) {
    if (item instanceof OAndBlock) {
      return (OAndBlock) item;
    }
    OAndBlock result = new OAndBlock(-1);
    result.subBlocks.add(item);
    return result;
  }

  public abstract boolean needsAliases(Set<String> aliases);

  public abstract OBooleanExpression copy();

  public boolean isEmpty() {
    return false;
  }

  public abstract void extractSubQueries(SubQueryCollector collector);

  public abstract boolean refersToParent();

  /**
   * returns the equivalent of current condition as an UPDATE expression with the same syntax, if
   * possible.
   *
   * <p>Eg. name = 3 can be considered a condition or an assignment. This method transforms the
   * condition in an assignment. This is used mainly for UPSERT operations.
   *
   * @return the equivalent of current condition as an UPDATE expression with the same syntax, if
   *     possible.
   */
  public Optional<OUpdateItem> transformToUpdateItem() {
    return Optional.empty();
  }

  public abstract List<String> getMatchPatternInvolvedAliases();

  public void translateLuceneOperator() {}

  public static OBooleanExpression deserializeFromOResult(OResult doc) {
    try {
      OBooleanExpression result =
          (OBooleanExpression)
              Class.forName(doc.getProperty("__class"))
                  .getConstructor(Integer.class)
                  .newInstance(-1);
      result.deserialize(doc);
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
    return null;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("__class", getClass().getName());
    return result;
  }

  public void deserialize(OResult fromResult) {
    throw new UnsupportedOperationException();
  }

  public abstract boolean isCacheable();

  public OBooleanExpression rewriteIndexChainsAsSubqueries(OCommandContext ctx, OClass clazz) {
    return this;
  }

  /**
   * returns true only if the expression does not need any further evaluation (eg. "true") and
   * always evaluates to true. It is supposed to be used as and optimization, and is allowed to
   * return false negatives
   *
   * @return
   */
  public boolean isAlwaysTrue() {
    return false;
  }

  public boolean isIndexAware(OIndexSearchInfo info) {
    return false;
  }

  public boolean createRangeWith(OBooleanExpression match) {
    return false;
  }

  public boolean isFullTextIndexAware(String indexField) {
    return false;
  }

  public OExpression resolveKeyFrom(OBinaryCondition additional) {
    throw new UnsupportedOperationException("Cannot execute index query with " + this);
  }

  public OExpression resolveKeyTo(OBinaryCondition additional) {
    throw new UnsupportedOperationException("Cannot execute index query with " + this);
  }
}
