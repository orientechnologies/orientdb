/* Generated By:JJTree: Do not edit this line. OGroupBy.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.ArrayList;
import java.util.List;

public class OGroupBy extends SimpleNode {

  protected List<OIdentifier> items = new ArrayList<OIdentifier>();

  public OGroupBy(int id) {
    super(id);
  }

  public OGroupBy(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("GROUP BY ");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        result.append(", ");
      }
      result.append(items.get(i).toString());
    }
    return result.toString();
  }
}
/* JavaCC - OriginalChecksum=4739190aa6c1a3533a89b76a15bd6fdf (do not edit this line) */
