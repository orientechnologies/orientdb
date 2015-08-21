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

  public void toString(Map<Object, Object> params, StringBuilder builder) {

    if (alias != null) {
      builder.append(alias);
      if (modifier != null) {
        modifier.toString(params, builder);
      }
    } else if (recordAttr != null) {
      builder.append(recordAttr);
    } else if (rid != null) {
      rid.toString(params, builder);
    }
    if (type != null) {
      builder.append(" " + type);
    }
  }
}
