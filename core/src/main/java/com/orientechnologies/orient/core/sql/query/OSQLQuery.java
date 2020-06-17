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

import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * SQL query implementation.
 *
 * @param <T> Record type to return.
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("serial")
public abstract class OSQLQuery<T> extends OQueryAbstract<T> implements OCommandRequestText {
  protected String text;

  public OSQLQuery() {}

  public OSQLQuery(final String iText) {
    text = iText.trim();
  }

  /** Delegates to the OQueryExecutor the query execution. */
  @SuppressWarnings("unchecked")
  public List<T> run(final Object... iArgs) {
    final ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().get();
    if (database == null) throw new OQueryParsingException("No database configured");

    ((OMetadataInternal) database.getMetadata()).makeThreadLocalSchemaSnapshot();
    try {
      setParameters(iArgs);
      Object o = database.getStorage().command(this);
      if (o instanceof List) {
        return (List<T>) o;
      } else {
        return (List<T>) Collections.singletonList(o);
      }

    } finally {
      ((OMetadataInternal) database.getMetadata()).clearThreadLocalSchemaSnapshot();
    }
  }

  /** Returns only the first record if any. */
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

  public OCommandRequestText fromStream(final byte[] iStream, ORecordSerializer serializer)
      throws OSerializationException {
    final OMemoryStream buffer = new OMemoryStream(iStream);

    queryFromStream(buffer, serializer);

    return this;
  }

  public byte[] toStream() throws OSerializationException {
    return queryToStream().toByteArray();
  }

  protected OMemoryStream queryToStream() {
    final OMemoryStream buffer = new OMemoryStream();

    buffer.setUtf8(text); // TEXT AS STRING
    buffer.set(limit); // LIMIT AS INTEGER
    buffer.setUtf8(fetchPlan != null ? fetchPlan : "");

    buffer.set(serializeQueryParameters(parameters));

    return buffer;
  }

  protected void queryFromStream(final OMemoryStream buffer, ORecordSerializer serializer) {
    text = buffer.getAsString();
    limit = buffer.getAsInteger();

    setFetchPlan(buffer.getAsString());

    final byte[] paramBuffer = buffer.getAsByteArray();
    parameters = deserializeQueryParameters(paramBuffer, serializer);
  }

  protected Map<Object, Object> deserializeQueryParameters(
      final byte[] paramBuffer, ORecordSerializer serializer) {
    if (paramBuffer == null || paramBuffer.length == 0) return Collections.emptyMap();

    final ODocument param = new ODocument();

    OImmutableSchema schema =
        ODatabaseRecordThreadLocal.instance().get().getMetadata().getImmutableSchemaSnapshot();
    serializer.fromStream(paramBuffer, param, null);
    param.setFieldType("params", OType.EMBEDDEDMAP);
    final Map<String, Object> params = param.rawField("params");

    final Map<Object, Object> result = new HashMap<Object, Object>();
    for (Entry<String, Object> p : params.entrySet()) {
      if (Character.isDigit(p.getKey().charAt(0)))
        result.put(Integer.parseInt(p.getKey()), p.getValue());
      else result.put(p.getKey(), p.getValue());
    }
    return result;
  }

  protected byte[] serializeQueryParameters(final Map<Object, Object> params) {
    if (params == null || params.size() == 0)
      // NO PARAMETER, JUST SEND 0
      return OCommonConst.EMPTY_BYTE_ARRAY;

    final ODocument param = new ODocument();
    param.field("params", convertToRIDsIfPossible(params));
    return param.toStream();
  }

  @SuppressWarnings("unchecked")
  private Map<Object, Object> convertToRIDsIfPossible(final Map<Object, Object> params) {
    final Map<Object, Object> newParams = new HashMap<Object, Object>(params.size());

    for (Entry<Object, Object> entry : params.entrySet()) {
      final Object value = entry.getValue();

      if (value instanceof Set<?>
          && !((Set<?>) value).isEmpty()
          && ((Set<?>) value).iterator().next() instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final Set<ORID> newSet = new HashSet<ORID>();
        for (ORecord rec : (Set<ORecord>) value) {
          newSet.add(rec.getIdentity());
        }
        newParams.put(entry.getKey(), newSet);

      } else if (value instanceof List<?>
          && !((List<?>) value).isEmpty()
          && ((List<?>) value).get(0) instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final List<ORID> newList = new ArrayList<ORID>();
        for (ORecord rec : (List<ORecord>) value) {
          newList.add(rec.getIdentity());
        }
        newParams.put(entry.getKey(), newList);

      } else if (value instanceof Map<?, ?>
          && !((Map<?, ?>) value).isEmpty()
          && ((Map<?, ?>) value).values().iterator().next() instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final Map<Object, ORID> newMap = new HashMap<Object, ORID>();
        for (Entry<?, ORecord> mapEntry : ((Map<?, ORecord>) value).entrySet()) {
          newMap.put(mapEntry.getKey(), mapEntry.getValue().getIdentity());
        }
        newParams.put(entry.getKey(), newMap);
      } else if (value instanceof OIdentifiable) {
        newParams.put(entry.getKey(), ((OIdentifiable) value).getIdentity());
      } else newParams.put(entry.getKey(), value);
    }

    return newParams;
  }
}
