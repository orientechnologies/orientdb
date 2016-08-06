package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

import java.util.Map;

/**
 * Created by luigidellaquila on 06/02/15.
 */
public class OOrderByItem {
  public static final String ASC  = "ASC";
  public static final String DESC = "DESC";
  protected String    alias;
  protected OModifier modifier;
  protected String    recordAttr;
  protected ORid      rid;
  protected String type = ASC;

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

  public int compare(OResult a, OResult b, OCommandContext ctx) {
    Object aVal = null;
    Object bVal = null;
    if (rid != null) {
      throw new UnsupportedOperationException("ORDER BY " + rid + " is not supported yet");
    }

    int result = 0;
    if (recordAttr != null) {
      aVal = a.getProperty(recordAttr);
      bVal = b.getProperty(recordAttr);
    } else if (alias != null) {
      aVal = a.getProperty(alias);
      bVal = b.getProperty(alias);
    }
    if (modifier != null) {
      aVal = modifier.execute(a, aVal, ctx);
      bVal = modifier.execute(b, bVal, ctx);
    }
    if (aVal == null) {
      if (bVal == null) {
        result = 0;
      } else {
        result = 1;
      }
    }
    if (bVal == null) {
      result = -1;
    }
    if (aVal instanceof Comparable) {
      try {
        result = ((Comparable) aVal).compareTo(bVal);
      } catch (Exception e) {
        result = 0;
      }
    }
    if (type == DESC) {
      result = -1 * result;
    }
    return result;
  }

  public OOrderByItem copy() {
    OOrderByItem result = new OOrderByItem();
    result.alias = alias;
    result.modifier = modifier == null ? null : modifier.copy();
    result.recordAttr = recordAttr;
    result.rid = rid == null ? null : rid.copy();
    result.type = type;
    return result;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (modifier != null) {
      modifier.extractSubQueries(collector);
    }
  }

  public boolean refersToParent() {
    if (alias != null && alias.equalsIgnoreCase("$parent")) {
      return true;
    }
    if (modifier != null && modifier.refersToParent()) {
      return true;
    }
    return false;
  }

  public OModifier getModifier() {
    return modifier;
  }
}
