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
package com.orientechnologies.orient.core.sql.query;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OMemoryStream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * SQL synchronous query. When executed the caller wait for the result.
 *
 * @param <T>
 *
 * @author Luca Garulli
 * @see OSQLAsynchQuery
 */
@SuppressWarnings({ "unchecked", "serial" })
public class OSQLSynchQuery<T> extends OSQLAsynchQuery<T> implements OCommandResultListener, Iterable<T> {
  private final OResultSet<T> result;
  private       ORID          nextPageRID;
  private Map<Object, Object> previousQueryParams = new HashMap<Object, Object>();

  public OSQLSynchQuery() {
    result = new OBasicResultSet<T>(null);
    resultListener = this;
  }

  public OSQLSynchQuery(final String iText) {
    super(iText);
    result = new OBasicResultSet<T>(iText);
    resultListener = this;
  }

  public OSQLSynchQuery(final String iText, final int iLimit) {
    super(iText, iLimit, null);
    result = new OBasicResultSet<T>(iText);
    resultListener = this;
  }

  @Override
  public void reset() {
    result.clear();
  }

  public boolean result(final Object iRecord) {
    if (iRecord != null)
      result.add((T) iRecord);
    return true;
  }

  @Override
  public void end() {
    result.setCompleted();
  }

  @Override
  public List<T> run(final Object... iArgs) {
    String currentThreadName = Thread.currentThread().getName();
    try {
      if (currentThreadName == null || !(currentThreadName.contains("<query>") || currentThreadName.contains("<command>"))) {
        if (currentThreadName == null) {
          currentThreadName = "";
        }

        try {
          Thread.currentThread().setName(currentThreadName + " <query>" + this.getText() + "</query>");
        } catch (SecurityException x) {
          // ignore, current thread for some reason cannot change its name
        }
      }

      result.clear();

      final Map<Object, Object> queryParams;
      queryParams = fetchQueryParams(iArgs);
      resetNextRIDIfParametersWereChanged(queryParams);

      final List<Object> res = (List<Object>) super.run(iArgs);

      if (res != result && res != null && result.isEmptyNoWait()) {
        for (Object item : res) {
          result.add((T) item);
        }
      }

      ((OResultSet) result).setCompleted();

      if (!result.isEmpty()) {
        previousQueryParams = new HashMap<Object, Object>(queryParams);
        final ORID lastRid = ((OIdentifiable) result.get(result.size() - 1)).getIdentity();
        nextPageRID = new ORecordId(lastRid.next());
      }

      //hack to prevent of removal of SQL text if Error is thrown
      try {
        Thread.currentThread().setName(currentThreadName);
      } catch (SecurityException x) {
        // ignore, current thread for some reason cannot change its name
      }

      return result;
    } catch (RuntimeException e) {
      //hack to prevent of removal of SQL text if Error is thrown
      try {
        Thread.currentThread().setName(currentThreadName);
      } catch (SecurityException x) {
        // ignore, current thread for some reason cannot change its name
      }

      throw e;
    }
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  public Object getResult() {
    return result;
  }

  /**
   * @return RID of the record that will be processed first during pagination mode.
   */
  public ORID getNextPageRID() {
    return nextPageRID;
  }

  public void resetPagination() {
    nextPageRID = null;
  }

  public Iterator<T> iterator() {
    execute();
    return ((Iterable<T>) getResult()).iterator();
  }

  @Override
  public boolean isAsynchronous() {
    return false;
  }

  @SuppressWarnings("deprecation")
  @Override
  protected OMemoryStream queryToStream() {
    final OMemoryStream buffer = super.queryToStream();

    buffer.setUtf8(nextPageRID != null ? nextPageRID.toString() : "");

    final byte[] queryParams = serializeQueryParameters(previousQueryParams);
    buffer.set(queryParams);

    return buffer;
  }

  @Override
  protected void queryFromStream(@SuppressWarnings("deprecation") final OMemoryStream buffer) {
    super.queryFromStream(buffer);

    final String rid = buffer.getAsString();
    if ("".equals(rid))
      nextPageRID = null;
    else
      nextPageRID = new ORecordId(rid);

    final byte[] serializedPrevParams = buffer.getAsByteArray();
    previousQueryParams = deserializeQueryParameters(serializedPrevParams);

  }

  private void resetNextRIDIfParametersWereChanged(final Map<Object, Object> queryParams) {
    if (!queryParams.equals(previousQueryParams))
      nextPageRID = null;
  }

  private Map<Object, Object> fetchQueryParams(final Object... iArgs) {
    if (iArgs != null && iArgs.length > 0)
      return convertToParameters(iArgs);

    Map<Object, Object> queryParams = getParameters();
    if (queryParams == null)
      queryParams = new HashMap<Object, Object>();
    return queryParams;
  }

}
