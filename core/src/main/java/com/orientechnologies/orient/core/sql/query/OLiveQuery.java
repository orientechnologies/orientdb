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
package com.orientechnologies.orient.core.sql.query;

/**
 * SQL live query.
 * <br/><br/>
 * The statement syntax is the same as a normal SQL SELECT statement, but with LIVE as prefix:
 * <br/><br/>
 * LIVE SELECT FROM Foo WHERE name = 'bar'
 * <br/><br/>
 * Executing this query, the caller will subscribe to receive changes happening in the database,
 * that match this query condition. The query returns a query token in the result set. To unsubscribe,
 * the user has to execute another live query with the following syntax:
 * <br/><br/>
 * LIVE UNSUBSCRIBE &lt;token&gt;
 * <br/><br/>
 * The callback passed as second parameter will be invoked every time a record is created/updated/deleted
 * and it matches the query conditions.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OLiveQuery<T> extends OSQLSynchQuery<T> {

  public OLiveQuery() {
  }

  public OLiveQuery(String iText, final OLiveResultListener iResultListener) {
    super(iText);
    setResultListener(new OLocalLiveResultListener(iResultListener));
  }

  @Override
  public <RET> RET execute(Object... iArgs) {
    return super.execute(iArgs);
  }
}
