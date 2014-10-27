/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */

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
