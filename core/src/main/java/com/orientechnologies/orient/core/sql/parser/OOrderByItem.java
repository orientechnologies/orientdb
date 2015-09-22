package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

/**
 * Created by luigidellaquila on 06/02/15.
 */
public class OOrderByItem {
  public static final String ASC  = "ASC";
  public static final String DESC = "DESC";
  protected String           alias;
  protected OModifier        modifier;
  protected String           recordAttr;
  protected ORid             rid;
  protected String           type = ASC;

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getRecordAttr() {
    return recordAttr;
  }

  public void setRecordAttr(String recordAttr) {
    this.recordAttr = recordAttr;
  }

  public ORid getRid() {
    return rid;
  }

  public void setRid(ORid rid) {
    this.rid = rid;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (alias != null) {
      result.append(alias);
      if (modifier != null) {
        result.append(modifier.toString());
      }
    } else if (recordAttr != null) {
      result.append(recordAttr);
    } else if (rid != null) {
      result.append(rid.toString());
    }
    if (type != null) {
      result.append(" " + type);
    }
    return result.toString();
  }

  public void replaceParameters(Map<Object, Object> params) {
    if(modifier!=null){
      modifier.replaceParameters(params);
    }
  }
}
