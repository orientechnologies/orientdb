/* Generated By:JJTree: Do not edit this line. OGtOperator.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.metadata.schema.OType;

public class OGtOperator extends SimpleNode implements OBinaryCompareOperator {
  public OGtOperator(int id) {
    super(id);
  }

  public OGtOperator(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public boolean execute(Object iLeft, Object iRight) {
    if (iLeft.getClass() != iRight.getClass() && iLeft instanceof Number && iRight instanceof Number) {
      Number[] couple = OType.castComparableNumber((Number) iLeft, (Number) iRight);
      iLeft = couple[0];
      iRight = couple[1];
    } else {
      iRight = OType.convert(iRight, iLeft.getClass());
    }
    if (iRight == null)
      return false;
    return ((Comparable<Object>) iLeft).compareTo(iRight) > 0;
  }

  @Override
  public String toString() {
    return ">";
  }

  @Override public boolean supportsBasicCalculation() {
    return true;
  }


}
/* JavaCC - OriginalChecksum=4b96739fc6e9ae496916d542db361376 (do not edit this line) */
