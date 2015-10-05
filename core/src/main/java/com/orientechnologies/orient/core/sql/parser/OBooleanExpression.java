package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by luigidellaquila on 07/11/14.
 */
public abstract class OBooleanExpression extends SimpleNode {

  public static OBooleanExpression TRUE = new OBooleanExpression(0) {
    @Override
    public boolean evaluate(OIdentifiable currentRecord, OCommandContext ctx) {
      return true;
    }


    @Override protected boolean supportsBasicCalculation() {
      return true;
    }

    @Override protected int getNumberOfExternalCalculations() {
      return 0;
    }

    @Override protected List<Object> getExternalCalculationConditions() {
      return Collections.EMPTY_LIST;
    }

    @Override
    public String toString() {
      return "true";
    }

    public void toString(Map<Object, Object> params, StringBuilder builder) {
      builder.append("true");
    }
  };

  public static OBooleanExpression FALSE = new OBooleanExpression(0) {
    @Override
    public boolean evaluate(OIdentifiable currentRecord, OCommandContext ctx) {
      return false;
    }


    @Override protected boolean supportsBasicCalculation() {
      return true;
    }

    @Override protected int getNumberOfExternalCalculations() {
      return 0;
    }

    @Override protected List<Object> getExternalCalculationConditions() {
      return Collections.EMPTY_LIST;
    }

    @Override
    public String toString() {
      return "false";
    }

    public void toString(Map<Object, Object> params, StringBuilder builder) {
      builder.append("false");
    }

  };

  public OBooleanExpression(int id) {
    super(id);
  }

  public OBooleanExpression(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public abstract boolean evaluate(OIdentifiable currentRecord, OCommandContext ctx);


    /**
     *
     * @return true if this expression can be calculated in plain Java, false otherwise (eg. LUCENE operator)
     */
    protected abstract boolean supportsBasicCalculation();

    /**
     *
     * @return the number of sub-expressions that have to be calculated using an external engine (eg. LUCENE)
     */
    protected abstract int getNumberOfExternalCalculations();

    /**
     *
     * @return the sub-expressions that have to be calculated using an external engine (eg. LUCENE)
     */
    protected abstract List<Object> getExternalCalculationConditions();

  public List<OBinaryCondition> getIndexedFunctionConditions(OClass iSchemaClass, ODatabaseDocumentInternal database) {
    return null;
  }

  public List<OAndBlock> flatten() {

    return Collections.singletonList(encapsulateInAndBlock(this));
  }

  protected OAndBlock encapsulateInAndBlock(OBooleanExpression item) {
    if(item instanceof OAndBlock){
      return (OAndBlock)item;
    }
    OAndBlock result = new OAndBlock(-1);
    result.subBlocks.add(item);
    return result;
  }

}