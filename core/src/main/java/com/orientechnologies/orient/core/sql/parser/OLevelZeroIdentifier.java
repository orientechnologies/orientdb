/* Generated By:JJTree: Do not edit this line. OLevelZeroIdentifier.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Map;

public class OLevelZeroIdentifier extends SimpleNode {
  protected OFunctionCall functionCall;
  protected Boolean       self;
  protected OCollection   collection;

  public OLevelZeroIdentifier(int id) {
    super(id);
  }

  public OLevelZeroIdentifier(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (functionCall != null) {
      functionCall.toString(params, builder);
    } else if (Boolean.TRUE.equals(self)) {
      builder.append("@this");
    } else if (collection != null) {
      collection.toString(params, builder);
    }
  }


  public Object execute(OIdentifiable iCurrentRecord, OCommandContext ctx) {
    if (functionCall != null) {
      return functionCall.execute(iCurrentRecord, ctx);
    }
    throw new UnsupportedOperationException();
  }
}
/* JavaCC - OriginalChecksum=0305fcf120ba9395b4c975f85cdade72 (do not edit this line) */
