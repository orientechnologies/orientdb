/* Generated By:JJTree: Do not edit this line. OFloatingPoint.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

public class OFloatingPoint extends ONumber {

  protected int    sign        = 1;
  protected String stringValue = null;

  public OFloatingPoint(int id) {
    super(id);
  }

  public OFloatingPoint(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public Number getValue() {
    return Double.parseDouble((sign == -1 ? "-" : "") + stringValue);
  }

  public int getSign() {
    return sign;
  }

  public void setSign(int sign) {
    this.sign = sign;
  }

  public String getStringValue() {
    return stringValue;
  }

  public void setStringValue(String stringValue) {
    this.stringValue = stringValue;
  }

  @Override
  public String toString() {
    if (sign == -1) {
      return '-' + stringValue;
    }
    return stringValue;
  }
}
/* JavaCC - OriginalChecksum=46acfb589f666717595e28f1b19611ae (do not edit this line) */
