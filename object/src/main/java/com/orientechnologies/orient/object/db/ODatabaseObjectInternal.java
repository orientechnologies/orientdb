package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;

public interface ODatabaseObjectInternal extends ODatabaseObject {

  public static ODatabaseObjectInternal get(ODatabaseObject doj) {
    return (ODatabaseObjectInternal) doj;
  }

  /** Start to visit an object and check if has been visited already
   *
   * @param obj
   * @return true if visited otherwise false
   */
  boolean startVisit(Object obj);

  void endVisit(Object iRecord);
}
