package com.orientechnologies.orient.jdbc;

import java.sql.RowId;

import com.orientechnologies.orient.core.id.ORID;

public class OrientRowId implements RowId {

  protected final ORID rid;

  public OrientRowId(final ORID rid) {
    this.rid = rid;
  }

  @Override
  public byte[] getBytes() {
    return rid.toStream();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof OrientRowId)
      return rid.equals(((OrientRowId) obj).rid);
    return false;
  }

  @Override
  public int hashCode() {
    return rid.hashCode();
  }

  @Override
  public String toString() {
    return rid.toString();
  }
}
