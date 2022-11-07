package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Locale;
import java.util.Map;

/** Created by luigidellaquila on 06/02/15. */
public class OOrderByItem {
  public static final String ASC = "ASC";
  public static final String DESC = "DESC";
  protected String alias;
  protected OModifier modifier;
  protected String recordAttr;
  protected ORid rid;
  protected String type = ASC;
  protected OExpression collate;

  // calculated at run time
  private OCollate collateStrategy;

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
    if (collate != null) {
      builder.append(" COLLATE ");
      collate.toString(params, builder);
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
    if (aVal == null && bVal == null) {
      aVal = a.getMetadata(alias);
      bVal = b.getMetadata(alias);
    }
    if (modifier != null) {
      aVal = modifier.execute(a, aVal, ctx);
      bVal = modifier.execute(b, bVal, ctx);
    }
    if (collate != null && collateStrategy == null) {
      Object collateVal = collate.execute(new OResultInternal(), ctx);
      if (collateVal == null) {
        collateVal = collate.toString();
        if (collateVal.equals("null")) {
          collateVal = null;
        }
      }
      if (collateVal != null) {
        collateStrategy = OSQLEngine.getCollate(String.valueOf(collateVal));
        if (collateStrategy == null) {
          collateStrategy =
              OSQLEngine.getCollate(String.valueOf(collateVal).toUpperCase(Locale.ENGLISH));
        }
        if (collateStrategy == null) {
          collateStrategy =
              OSQLEngine.getCollate(String.valueOf(collateVal).toLowerCase(Locale.ENGLISH));
        }
        if (collateStrategy == null) {
          throw new OCommandExecutionException("Invalid collate for ORDER BY: " + collateVal);
        }
      }
    }

    if (collateStrategy != null) {
      result = collateStrategy.compareForOrderBy(aVal, bVal);
    } else {
      if (aVal == null) {
        if (bVal == null) {
          result = 0;
        } else {
          result = -1;
        }
      } else if (bVal == null) {
        result = 1;
      } else if (aVal instanceof Comparable && bVal instanceof Comparable) {
        try {
          result = ((Comparable) aVal).compareTo(bVal);
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during comparision", e);
          result = 0;
        }
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
    result.collate = this.collate == null ? null : collate.copy();
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
    if (collate != null && collate.refersToParent()) {
      return true;
    }
    return false;
  }

  public OModifier getModifier() {
    return modifier;
  }

  public void setModifier(OModifier modifier) {
    this.modifier = modifier;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("alias", alias);
    if (modifier != null) {
      result.setProperty("modifier", modifier.serialize());
    }
    result.setProperty("recordAttr", recordAttr);
    if (rid != null) {
      result.setProperty("rid", rid.serialize());
    }
    result.setProperty("type", type);
    if (collate != null) {
      result.setProperty("collate", collate.serialize());
    }
    return result;
  }

  public void deserialize(OResult fromResult) {
    alias = fromResult.getProperty("alias");
    if (fromResult.getProperty("modifier") != null) {
      modifier = new OModifier(-1);
      modifier.deserialize(fromResult.getProperty("modifier"));
    }
    recordAttr = fromResult.getProperty("recordAttr");
    if (fromResult.getProperty("rid") != null) {
      rid = new ORid(-1);
      rid.deserialize(fromResult.getProperty("rid"));
    }
    type = DESC.equals(fromResult.getProperty("type")) ? DESC : ASC;
    if (fromResult.getProperty("collate") != null) {
      collate = new OExpression(-1);
      collate.deserialize(fromResult.getProperty("collate"));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OOrderByItem that = (OOrderByItem) o;

    if (alias != null ? !alias.equals(that.alias) : that.alias != null) return false;
    if (modifier != null ? !modifier.equals(that.modifier) : that.modifier != null) return false;
    if (recordAttr != null ? !recordAttr.equals(that.recordAttr) : that.recordAttr != null)
      return false;
    if (rid != null ? !rid.equals(that.rid) : that.rid != null) return false;
    if (type != null ? !type.equals(that.type) : that.type != null) return false;
    return collate != null ? collate.equals(that.collate) : that.collate == null;
  }

  @Override
  public int hashCode() {
    int result = alias != null ? alias.hashCode() : 0;
    result = 31 * result + (modifier != null ? modifier.hashCode() : 0);
    result = 31 * result + (recordAttr != null ? recordAttr.hashCode() : 0);
    result = 31 * result + (rid != null ? rid.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    result = 31 * result + (collate != null ? collate.hashCode() : 0);
    return result;
  }

  public OExpression getCollate() {
    return collate;
  }

  public void toGenericStatement(StringBuilder builder) {

    if (alias != null) {
      builder.append(alias);
      if (modifier != null) {
        modifier.toGenericStatement(builder);
      }
    } else if (recordAttr != null) {
      builder.append(recordAttr);
    } else if (rid != null) {
      rid.toGenericStatement(builder);
    }
    if (type != null) {
      builder.append(" " + type);
    }
    if (collate != null) {
      builder.append(" COLLATE ");
      collate.toGenericStatement(builder);
    }
  }
}
