/* Generated By:JJTree: Do not edit this line. OFromItem.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.List;
import java.util.Map;

public class OFromItem extends SimpleNode {

  protected List<ORid>          rids;
  protected OCluster            cluster;
  protected OClusterList        clusterList;
  // protected OIdentifier className;
  protected OIndexIdentifier    index;
  protected OMetadataIdentifier metadata;
  protected OStatement          statement;
  protected OInputParameter     inputParam;
  protected OBaseIdentifier     identifier;
  protected OModifier           modifier;

  private static final Object   UNSET           = new Object();
  private Object                inputFinalValue = UNSET;

  public OFromItem(int id) {
    super(id);
  }

  public OFromItem(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (rids != null && rids.size() > 0) {
      if (rids.size() == 1) {
        rids.get(0).toString(params, builder);
        return;
      } else {
        builder.append("[");
        boolean first = true;
        for (ORid rid : rids) {
          if (!first) {
            builder.append(", ");
          }
          rid.toString(params, builder);
          first = false;
        }
        builder.append("]");
        return;
      }
    } else if (cluster != null) {
      cluster.toString(params, builder);
      return;
      // } else if (className != null) {
      // return className.getValue();
    } else if (clusterList != null) {
      clusterList.toString(params, builder);
      return;
    } else if (metadata != null) {
      metadata.toString(params, builder);
      return;
    } else if (statement != null) {
      builder.append("(");
      statement.toString(params, builder);
      builder.append(")");
      return;
    } else if (index != null) {
      index.toString(params, builder);
      return;
    } else if (inputParam != null) {
      inputParam.toString(params, builder);
    } else if (identifier != null) {

      identifier.toString(params, builder);
      if (modifier != null) {
        modifier.toString(params, builder);
      }
      return;
    }
  }


  public OBaseIdentifier getIdentifier() {
    return identifier;
  }
}
/* JavaCC - OriginalChecksum=f64e3b4d2a2627a1b5d04a7dcb95fa94 (do not edit this line) */
