/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.query;

import java.io.Externalizable;
import java.util.List;

/**
 * ResultSet interface that extends List interface for retro compatibility.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @param <T>
 * @see OSQLAsynchQuery
 */
public interface OLegacyResultSet<T> extends List<T>, Externalizable {

  OLegacyResultSet<T> setCompleted();

  int getLimit();

  OLegacyResultSet<T> setLimit(int limit);

  OLegacyResultSet<T> copy();

  boolean isEmptyNoWait();

  /**
   * Returns the current size. If the resultset is not yet ready, returns te current size.
   *
   * @return
   */
  int currentSize();
}
