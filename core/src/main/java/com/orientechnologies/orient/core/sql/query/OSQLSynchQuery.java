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
 * @author Luca Garulli
 * 
 * @param <T>
 * @see OSQLAsynchQuery
 */
@SuppressWarnings({ "unchecked", "serial" })
public class OSQLSynchQuery<T extends Object> extends OSQLAsynchQuery<T> implements OCommandResultListener, Iterable<T> {
  private final OResultSet<T> result              = new OResultSet<T>();
  private ORID                nextPageRID;
  private Map<Object, Object> previousQueryParams = new HashMap<Object, Object>();

  public OSQLSynchQuery() {
    resultListener = this;
  }

  public OSQLSynchQuery(final String iText) {
    super(iText);
    resultListener = this;
  }

  public OSQLSynchQuery(final String iText, final int iLimit) {
    super(iText, iLimit, null);
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
    result.clear();

    final Map<Object, Object> queryParams;
    queryParams = fetchQueryParams(iArgs);
    resetNextRIDIfParametersWereChanged(queryParams);

    final List<Object> res = (List<Object>) super.run(iArgs);

    if (res != result && res != null && result.isEmptyNoWait()) {
      Iterator<Object> iter = res.iterator();
      while (iter.hasNext()) {
        Object item = iter.next();
        result.add((T) item);
      }
    }

    ((OResultSet) result).setCompleted();

    if (!result.isEmpty()) {
      previousQueryParams = new HashMap<Object, Object>(queryParams);
      final ORID lastRid = ((OIdentifiable) result.get(result.size() - 1)).getIdentity();
      nextPageRID = new ORecordId(lastRid.next());
    }

    return result;
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

  @Override
  protected OMemoryStream queryToStream() {
    final OMemoryStream buffer = super.queryToStream();

    buffer.setUtf8(nextPageRID != null ? nextPageRID.toString() : "");

    final byte[] queryParams = serializeQueryParameters(previousQueryParams);
    buffer.set(queryParams);

    return buffer;
  }

  @Override
  protected void queryFromStream(OMemoryStream buffer) {
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
