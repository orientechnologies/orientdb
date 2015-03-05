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

import java.util.*;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * SQL query implementation.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 *          Record type to return.
 */
@SuppressWarnings("serial")
public abstract class OSQLQuery<T> extends OQueryAbstract<T> implements OCommandRequestText {
  protected String text;

  public OSQLQuery() {
  }

  public OSQLQuery(final String iText) {
    text = iText.trim();
  }

  /**
   * Delegates to the OQueryExecutor the query execution.
   */
  @SuppressWarnings("unchecked")
  public List<T> run(final Object... iArgs) {
    final ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (database == null) {
        throw new OQueryParsingException("No database configured");
    }

    ((OMetadataInternal) database.getMetadata()).makeThreadLocalSchemaSnapshot();
    try {
      setParameters(iArgs);
      return (List<T>) database.getStorage().command(this);

    } finally {
      ((OMetadataInternal) database.getMetadata()).clearThreadLocalSchemaSnapshot();
    }
  }

  /**
   * Returns only the first record if any.
   */
  public T runFirst(final Object... iArgs) {
    setLimit(1);
    final List<T> result = execute(iArgs);
    return result != null && !result.isEmpty() ? result.get(0) : null;
  }

  public String getText() {
    return text;
  }

  public OCommandRequestText setText(final String iText) {
    text = iText;
    return this;
  }

  @Override
  public String toString() {
    return "sql." + text;
  }

  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    final OMemoryStream buffer = new OMemoryStream(iStream);

    queryFromStream(buffer);

    return this;
  }

  public byte[] toStream() throws OSerializationException {
    return queryToStream().toByteArray();
  }

  protected OMemoryStream queryToStream() {
    final OMemoryStream buffer = new OMemoryStream();

    buffer.setUtf8(text); // TEXT AS STRING
    buffer.set(limit); // LIMIT AS INTEGER
    buffer.setUtf8(fetchPlan != null ? fetchPlan : ""); // FETCH PLAN IN FORM OF STRING (to know more goto:
    // http://code.google.com/p/orient/wiki/FetchingStrategies)

    buffer.set(serializeQueryParameters(parameters));

    return buffer;
  }

  protected void queryFromStream(final OMemoryStream buffer) {
    text = buffer.getAsString();
    limit = buffer.getAsInteger();

    setFetchPlan(buffer.getAsString());

    final byte[] paramBuffer = buffer.getAsByteArray();
    parameters = deserializeQueryParameters(paramBuffer);
  }

  protected Map<Object, Object> deserializeQueryParameters(final byte[] paramBuffer) {
    if (paramBuffer == null || paramBuffer.length == 0) {
        return Collections.emptyMap();
    }

    final ODocument param = new ODocument();
    param.fromStream(paramBuffer);
    param.setFieldType("params", OType.EMBEDDEDMAP);
    final Map<String, Object> params = param.rawField("params");

    final Map<Object, Object> result = new HashMap<Object, Object>();
    for (Entry<String, Object> p : params.entrySet()) {
      if (Character.isDigit(p.getKey().charAt(0))) {
          result.put(Integer.parseInt(p.getKey()), p.getValue());
      } else {
          result.put(p.getKey(), p.getValue());
      }
    }
    return result;
  }

  protected byte[] serializeQueryParameters(final Map<Object, Object> params) {
    if (params == null || params.size() == 0) {
        // NO PARAMETER, JUST SEND 0
        return new byte[0];
    }

    final ODocument param = new ODocument();
    param.field("params", convertToRIDsIfPossible(params));
    return param.toStream();
  }

  @SuppressWarnings("unchecked")
  private Map<Object, Object> convertToRIDsIfPossible(final Map<Object, Object> params) {
    final Map<Object, Object> newParams = new HashMap<Object, Object>(params.size());

    for (Entry<Object, Object> entry : params.entrySet()) {
      final Object value = entry.getValue();

      if (value instanceof Set<?> && ((Set<?>) value).iterator().next() instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final Set<ORID> newSet = new HashSet<ORID>();
        for (ORecord rec : (Set<ORecord>) value) {
          newSet.add(rec.getIdentity());
        }
        newParams.put(entry.getKey(), newSet);

      } else if (value instanceof List<?> && ((List<?>) value).get(0) instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final List<ORID> newList = new ArrayList<ORID>();
        for (ORecord rec : (List<ORecord>) value) {
          newList.add(rec.getIdentity());
        }
        newParams.put(entry.getKey(), newList);

      } else if (value instanceof Map<?, ?> && ((Map<?, ?>) value).values().iterator().next() instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final Map<Object, ORID> newMap = new HashMap<Object, ORID>();
        for (Entry<?, ORecord> mapEntry : ((Map<?, ORecord>) value).entrySet()) {
          newMap.put(mapEntry.getKey(), mapEntry.getValue().getIdentity());
        }
        newParams.put(entry.getKey(), newMap);
      } else if (entry.getValue() instanceof ORecord) {
        newParams.put(entry.getKey(), ((OIdentifiable) entry.getValue()).getIdentity());
      } else {
          newParams.put(entry.getKey(), entry.getValue());
      }
    }

    return newParams;
  }
}
