/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */

package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public interface OSQLFunctionFiltered extends OSQLFunction {

  /**
   * Process a record.
   *
   * @param iThis
   * @param iCurrentRecord : current record
   * @param iCurrentResult TODO
   * @param iParams : function parameters, number is ensured to be within minParams and maxParams.
   * @param iPossibleResults : a set of possible results (the function will return, as a result,
   *     only items contained in this collection)
   * @param iContext : object calling this function
   * @return function result, can be null. Special cases : can be null if function aggregate
   *     results, can be null if function filter results : this mean result is excluded
   */
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      Iterable<OIdentifiable> iPossibleResults,
      OCommandContext iContext);
}
