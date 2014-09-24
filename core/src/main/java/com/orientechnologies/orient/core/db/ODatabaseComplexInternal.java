package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;

public interface ODatabaseComplexInternal<T> extends ODatabaseComplex<T>, ODatabaseInternal {

  /**
   * Returns the database owner. Used in wrapped instances to know the up level ODatabase instance.
   * 
   * @return Returns the database owner.
   */
  public ODatabaseComplexInternal<?> getDatabaseOwner();

  /**
   * Internal. Sets the database owner.
   */
  public ODatabaseComplexInternal<?> setDatabaseOwner(ODatabaseComplexInternal<?> iOwner);

  /**
   * Return the underlying database. Used in wrapper instances to know the down level ODatabase instance.
   * 
   * @return The underlying ODatabase implementation.
   */
  public <DB extends ODatabase> DB getUnderlying();

  /**
   * Internal method. Don't call it directly unless you're building an internal component.
   */
  public void setInternal(ATTRIBUTES attribute, Object iValue);

}
