/* Generated By:JJTree: Do not edit this line. ORecordAttribute.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Map;

public class ORecordAttribute extends SimpleNode {

  protected String name;

  public ORecordAttribute(int id) {
    super(id);
  }

  public ORecordAttribute(OrientSql p, int id) {
    super(p, id);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(name);
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append(name);
  }

  public ORecordAttribute copy() {
    ORecordAttribute result = new ORecordAttribute(-1);
    result.name = name;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ORecordAttribute that = (ORecordAttribute) o;

    if (name != null ? !name.equals(that.name) : that.name != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return name != null ? name.hashCode() : 0;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("name", name);
    return result;
  }

  public void deserialize(OResult fromResult) {
    name = fromResult.getProperty("name");
  }

  public Object evaluate(OResult iCurrentRecord, OCommandContext ctx) {
    if (name.equalsIgnoreCase("@rid")) {
      ORID identity = iCurrentRecord.getIdentity().orElse(null);
      if (identity == null) {
        identity = iCurrentRecord.getProperty(name);
      }
      return identity;
    } else if (name.equalsIgnoreCase("@class")) {
      return iCurrentRecord
          .getElement()
          .flatMap(e -> e.getSchemaType())
          .map(x -> x.getName())
          .orElseGet(
              () -> {
                return iCurrentRecord.getProperty(name);
              });
    } else if (name.equalsIgnoreCase("@version")) {
      iCurrentRecord
          .getRecord()
          .map(r -> r.getVersion())
          .orElseGet(
              () -> {
                return iCurrentRecord.getProperty(name);
              });
    }
    return null;
  }

  public Object evaluate(OElement iCurrentRecord, OCommandContext ctx) {
    if (iCurrentRecord == null) {
      return null;
    }
    if (name.equalsIgnoreCase("@rid")) {
      return iCurrentRecord.getIdentity();
    } else if (name.equalsIgnoreCase("@class")) {
      return iCurrentRecord.getSchemaType().map(clazz -> clazz.getName()).orElse(null);
    } else if (name.equalsIgnoreCase("@version")) {
      ORecord record = iCurrentRecord.getRecord();
      if (record == null) {
        return null;
      }
      return record.getVersion();
    }
    return null;
  }
}
/* JavaCC - OriginalChecksum=45ce3cd16399dec7d7ef89f8920d02ae (do not edit this line) */
