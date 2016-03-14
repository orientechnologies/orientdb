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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Proxied abstract index.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public abstract class OIndexRemote<T> implements OIndex<T> {
  public static final    String QUERY_GET_VALUES_BEETWEN_SELECT                   = "select from index:%s where ";
  public static final    String QUERY_GET_VALUES_BEETWEN_INCLUSIVE_FROM_CONDITION = "key >= ?";
  public static final    String QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_FROM_CONDITION = "key > ?";
  public static final    String QUERY_GET_VALUES_BEETWEN_INCLUSIVE_TO_CONDITION   = "key <= ?";
  public static final    String QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_TO_CONDITION   = "key < ?";
  public static final    String QUERY_GET_VALUES_AND_OPERATOR                     = " and ";
  public static final    String QUERY_GET_VALUES_LIMIT                            = " limit ";
  protected final static String QUERY_ENTRIES                                     = "select key, rid from index:%s";
  protected final static String QUERY_ENTRIES_DESC                                = "select key, rid from index:%s order by key desc";

  private final static String QUERY_GET_ENTRIES = "select from index:%s where key in [%s]";

  private final static String QUERY_PUT         = "insert into index:%s (key,rid) values (?,?)";
  private final static String QUERY_REMOVE      = "delete from index:%s where key = ?";
  private final static String QUERY_REMOVE2     = "delete from index:%s where key = ? and rid = ?";
  private final static String QUERY_REMOVE3     = "delete from index:%s where rid = ?";
  private final static String QUERY_CONTAINS    = "select count(*) as size from index:%s where key = ?";
  private final static String QUERY_COUNT       = "select count(*) as size from index:%s where key = ?";
  private final static String QUERY_COUNT_RANGE = "select count(*) as size from index:%s where ";
  private final static String QUERY_SIZE        = "select count(*) as size from index:%s";
  private final static String QUERY_KEY_SIZE    = "select count(distinct( key )) as size from index:%s";
  private final static String QUERY_KEYS        = "select key from index:%s";
  private final static String QUERY_REBUILD     = "rebuild index %s";
  private final static String QUERY_CLEAR       = "delete from index:%s";
  private final static String QUERY_DROP        = "drop index %s";
  protected final String           databaseName;
  private final   String           wrappedType;
  private final   String           algorithm;
  protected       OIndexDefinition indexDefinition;
  protected       String           name;
  protected       ODocument        configuration;
  protected       Set<String>      clustersToIndex;

  public OIndexRemote(final String iName, final String iWrappedType, final String algorithm,
      final OIndexDefinition iIndexDefinition, final ODocument iConfiguration, final Set<String> clustersToIndex) {
    this.name = iName;
    this.wrappedType = iWrappedType;
    this.algorithm = algorithm;
    this.indexDefinition = iIndexDefinition;
    this.configuration = iConfiguration;
    this.clustersToIndex = new HashSet<String>(clustersToIndex);
    this.databaseName = ODatabaseRecordThreadLocal.INSTANCE.get().getName();
  }

  public OIndexRemote<T> create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    this.name = name;
    return this;
  }

  public OIndexRemote<T> delete() {
    final OCommandRequest cmd = formatCommand(QUERY_DROP, name);
    getDatabase().command(cmd).execute();
    return this;
  }

  @Override
  public void deleteWithoutIndexLoad(String indexName) {
    throw new UnsupportedOperationException("deleteWithoutIndexLoad");
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public boolean contains(final Object iKey) {
    final OCommandRequest cmd = formatCommand(QUERY_CONTAINS, name);
    final List<ODocument> result = getDatabase().command(cmd).execute(iKey);
    return (Long) result.get(0).field("size") > 0;
  }

  public long count(final Object iKey) {
    final OCommandRequest cmd = formatCommand(QUERY_COUNT, name);
    final List<ODocument> result = getDatabase().command(cmd).execute(iKey);
    return (Long) result.get(0).field("size");
  }

  public long count(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo, final boolean iToInclusive,
      final int maxValuesToFetch) {
    final StringBuilder query = new StringBuilder(QUERY_COUNT_RANGE);

    if (iFromInclusive)
      query.append(QUERY_GET_VALUES_BEETWEN_INCLUSIVE_FROM_CONDITION);
    else
      query.append(QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_FROM_CONDITION);

    query.append(QUERY_GET_VALUES_AND_OPERATOR);

    if (iToInclusive)
      query.append(QUERY_GET_VALUES_BEETWEN_INCLUSIVE_TO_CONDITION);
    else
      query.append(QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_TO_CONDITION);

    if (maxValuesToFetch > 0)
      query.append(QUERY_GET_VALUES_LIMIT).append(maxValuesToFetch);

    final OCommandRequest cmd = formatCommand(query.toString());
    return (Long) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
  }

  public OIndexRemote<T> put(final Object iKey, final OIdentifiable iValue) {
    if (iValue instanceof ORecord && !iValue.getIdentity().isValid())
      // SAVE IT BEFORE TO PUT
      ((ORecord) iValue).save();

    if (iValue.getIdentity().isNew())
      throw new OIndexException(
          "Cannot insert values in manual indexes against remote protocol during a transaction. Temporary RID cannot be managed at server side");

    final OCommandRequest cmd = formatCommand(QUERY_PUT, name);
    getDatabase().command(cmd).execute(iKey, iValue.getIdentity());
    return this;
  }

  public boolean remove(final Object key) {
    final OCommandRequest cmd = formatCommand(QUERY_REMOVE, name);
    return ((Integer) getDatabase().command(cmd).execute(key)) > 0;
  }

  public boolean remove(final Object iKey, final OIdentifiable iRID) {
    final int deleted;
    if (iRID != null) {

      if (iRID.getIdentity().isNew())
        throw new OIndexException(
            "Cannot remove values in manual indexes against remote protocol during a transaction. Temporary RID cannot be managed at server side");

      final OCommandRequest cmd = formatCommand(QUERY_REMOVE2, name);
      deleted = (Integer) getDatabase().command(cmd).execute(iKey, iRID);
    } else {
      final OCommandRequest cmd = formatCommand(QUERY_REMOVE, name);
      deleted = (Integer) getDatabase().command(cmd).execute(iKey);
    }
    return deleted > 0;
  }

  public int remove(final OIdentifiable iRecord) {
    final OCommandRequest cmd = formatCommand(QUERY_REMOVE3, name, iRecord.getIdentity());
    return (Integer) getDatabase().command(cmd).execute(iRecord);
  }

  public void automaticRebuild() {
    throw new UnsupportedOperationException("autoRebuild()");
  }

  public long rebuild() {
    final OCommandRequest cmd = formatCommand(QUERY_REBUILD, name);
    return (Long) getDatabase().command(cmd).execute();
  }

  public OIndexRemote<T> clear() {
    final OCommandRequest cmd = formatCommand(QUERY_CLEAR, name);
    getDatabase().command(cmd).execute();
    return this;
  }

  public long getSize() {
    final OCommandRequest cmd = formatCommand(QUERY_SIZE, name);
    final List<ODocument> result = getDatabase().command(cmd).execute();
    return (Long) result.get(0).field("size");
  }

  public long getKeySize() {
    final OCommandRequest cmd = formatCommand(QUERY_KEY_SIZE, name);
    final List<ODocument> result = getDatabase().command(cmd).execute();
    return (Long) result.get(0).field("size");
  }

  public boolean isAutomatic() {
    return indexDefinition != null && indexDefinition.getClassName() != null;
  }

  public String getName() {
    return name;
  }

  @Override
  public void flush() {
  }

  public String getType() {
    return wrappedType;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public ODocument getConfiguration() {
    return configuration;
  }

  @Override
  public ODocument getMetadata() {
    return configuration.field("metadata", OType.EMBEDDED);
  }

  public void commit(final ODocument iDocument) {
  }

  public OIndexInternal<T> getInternal() {
    return null;
  }

  public long rebuild(final OProgressListener iProgressListener) {
    return rebuild();
  }

  public OType[] getKeyTypes() {
    if (indexDefinition != null)
      return indexDefinition.getTypes();
    return null;
  }

  public Collection<ODocument> getEntries(final Collection<?> iKeys) {
    final StringBuilder params = new StringBuilder(128);
    if (!iKeys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < iKeys.size(); i++) {
        params.append(", ?");
      }
    }

    final OCommandRequest cmd = formatCommand(QUERY_GET_ENTRIES, name, params.toString());
    return (Collection<ODocument>) getDatabase().command(cmd).execute(iKeys.toArray());
  }

  public OIndexDefinition getDefinition() {
    return indexDefinition;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OIndexRemote<?> that = (OIndexRemote<?>) o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  public Collection<ODocument> getEntries(final Collection<?> iKeys, int maxEntriesToFetch) {
    if (maxEntriesToFetch < 0)
      return getEntries(iKeys);

    final StringBuilder params = new StringBuilder(128);
    if (!iKeys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < iKeys.size(); i++) {
        params.append(", ?");
      }
    }

    final OCommandRequest cmd = formatCommand(QUERY_GET_ENTRIES + QUERY_GET_VALUES_LIMIT + maxEntriesToFetch, name,
        params.toString());
    return getDatabase().command(cmd).execute(iKeys.toArray());
  }

  public Set<String> getClusters() {
    return Collections.unmodifiableSet(clustersToIndex);
  }

  public ODocument checkEntry(final OIdentifiable iRecord, final Object iKey) {
    return null;
  }

  @Override
  public boolean isRebuilding() {
    return false;
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("getFirstKey");
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException("getLastKey");
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesMinor");
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    throw new UnsupportedOperationException("iterateEntries");
  }

  @Override
  public OIndexCursor cursor() {
    final OCommandRequest cmd = formatCommand(QUERY_ENTRIES, name);
    final Collection<ODocument> result = getDatabase().command(cmd).execute();

    return new OIndexAbstractCursor() {
      private final Iterator<ODocument> documentIterator = result.iterator();

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (!documentIterator.hasNext())
          return null;

        final ODocument value = documentIterator.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return value.field("key");
          }

          @Override
          public OIdentifiable getValue() {
            return value.field("rid");
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };

  }

  @Override
  public OIndexCursor descCursor() {
    final OCommandRequest cmd = formatCommand(QUERY_ENTRIES_DESC, name);
    final Collection<ODocument> result = getDatabase().command(cmd).execute();

    return new OIndexAbstractCursor() {
      private final Iterator<ODocument> documentIterator = result.iterator();

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (!documentIterator.hasNext())
          return null;

        final ODocument value = documentIterator.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return value.field("key");
          }

          @Override
          public OIdentifiable getValue() {
            return value.field("rid");
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    final OCommandRequest cmd = formatCommand(QUERY_KEYS, name);
    final Collection<ODocument> result = getDatabase().command(cmd).execute();

    return new OIndexKeyCursor() {
      private final Iterator<ODocument> documentIterator = result.iterator();

      @Override
      public Map.Entry<Object, OIdentifiable> next(int prefetchSize) {
        if (!documentIterator.hasNext())
          return null;

        final ODocument value = documentIterator.next();

        return value.field("key");
      }
    };
  }

  @Override
  public int compareTo(OIndex<T> index) {
    final String name = index.getName();
    return this.name.compareTo(name);
  }

  protected OCommandRequest formatCommand(final String iTemplate, final Object... iArgs) {
    final String text = String.format(iTemplate, iArgs);
    return new OCommandSQL(text);
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override
  public long getRebuildVersion() {
    throw new UnsupportedOperationException();
  }
}
