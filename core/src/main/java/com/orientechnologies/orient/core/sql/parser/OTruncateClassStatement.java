/* Generated By:JJTree: Do not edit this line. OTruncateClassStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

public
class OTruncateClassStatement extends OStatement {

  protected OIdentifier className;
  protected boolean polymorphic = false;
  protected boolean unsafe = false;

  public OTruncateClassStatement(int id) {
    super(id);
  }

  public OTruncateClassStatement(OrientSql p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("TRUNCATE CLASS "+className.toString());
    if(polymorphic){
      builder.append(" POLYMORPHIC");
    }
    if(unsafe){
      builder.append(" UNSAFE");
    }
  }
}
/* JavaCC - OriginalChecksum=301f993f6ba2893cb30c8f189674b974 (do not edit this line) */
