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

import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.List;
import java.util.Map;

/**
 * SQL asynchronous query. When executed the caller does not wait for the execution, rather the
 * listener will be called for each item found in the query. OSQLAsynchQuery has been built on top
 * of this. NOTE: if you're working with remote databases don't execute any remote call inside the
 * callback function because the network channel is locked until the query command has finished.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @param <T>
 * @see OSQLSynchQuery
 */
public class OSQLAsynchQuery<T extends Object> extends OSQLQuery<T>
    implements OCommandRequestAsynch {
  private static final long serialVersionUID = 1L;

  /** Empty constructor for unmarshalling. */
  public OSQLAsynchQuery() {}

  public OSQLAsynchQuery(final String iText) {
    this(iText, null);
  }

  public OSQLAsynchQuery(final String iText, final OCommandResultListener iResultListener) {
    this(iText, -1, iResultListener);
  }

  public OSQLAsynchQuery(
      final String iText,
      final int iLimit,
      final String iFetchPlan,
      final Map<Object, Object> iArgs,
      final OCommandResultListener iResultListener) {
    this(iText, iLimit, iResultListener);
    this.fetchPlan = iFetchPlan;
    this.parameters = iArgs;
  }

  public OSQLAsynchQuery(
      final String iText, final int iLimit, final OCommandResultListener iResultListener) {
    super(iText);
    limit = iLimit;
    resultListener = iResultListener;
  }

  @Override
  public List<T> run(Object... iArgs) {
    if (resultListener == null)
      throw new OCommandExecutionException("Listener not found on asynch query");

    return super.run(iArgs);
  }

  /** Sets default non idempotent to avoid custom query deadlocks database. */
  @Override
  public boolean isIdempotent() {
    return true;
  }

  @Override
  public boolean isAsynchronous() {
    return true;
  }
}
