/* Generated By:JJTree: Do not edit this line. OInsertStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

public class OInsertStatement extends OStatement {

  OIdentifier      targetClass;
  OIdentifier      targetClusterName;
  OCluster         targetCluster;
  OIndexIdentifier targetIndex;
  OProjection      returnStatement;
  OInsertBody      insertBody;
  boolean          unsafe = false;

  public OInsertStatement(int id) {
    super(id);
  }

  public OInsertStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("INSERT INTO ");
    if (targetClass != null) {
      result.append(targetClass.toString());
      if (targetClusterName != null) {
        result.append(" CLUSTER ");
        result.append(targetClusterName.toString());
      }
    }
    if (targetCluster != null) {
      result.append(targetCluster.toString());
    }
    if (targetIndex != null) {
      result.append(targetIndex.toString());
    }
    if (returnStatement != null) {
      result.append(" RETURN ");
      result.append(returnStatement.toString());
    }
    if (insertBody != null) {
      result.append(' ');
      result.append(insertBody.toString());
    }
    if (unsafe) {
      result.append(" UNSAFE");
    }
    return result.toString();
  }

  public void replaceParameters(Map<Object, Object> params) {
    if (returnStatement != null) {
      returnStatement.replaceParameters(params);
    }
    if (insertBody != null) {
      insertBody.replaceParameters(params);
    }
  }

}
/* JavaCC - OriginalChecksum=ccfabcf022d213caed873e6256cb26ad (do not edit this line) */
