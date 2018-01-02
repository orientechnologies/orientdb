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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Proxied abstract index.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
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

  private final static String QUERY_ITERATE_ENTRIES = "select from index:%s where key in [%s] order by key %s ";
  private final static String QUERY_GET_ENTRIES     = "select from index:%s where key in [%s]";

  private final static String QUERY_PUT         = "insert into index:%s (key,rid) values (?,?)";
  private final static String QUERY_REMOVE      = "delete from index:%s where key = ?";
  private final static String QUERY_REMOVE2     = "delete from index:%s where key = ? and rid = ?";
  private final static String QUERY_REMOVE3     = "delete from index:%s where rid = ?";
  private final static String QUERY_CONTAINS    = "select count(*) as size from index:%s where key = ?";
  private final static String QUERY_COUNT       = "select count(*) as size from index:%s where key = ?";
  private final static String QUERY_COUNT_RANGE = "select count(*) as size from index:%s where ";
  private final static String QUERY_SIZE        = "select count(*) as size from index:%s";
  private final static String QUERY_KEY_SIZE    = "select indexKeySize('%s') as size";
  private final static String QUERY_KEYS        = "select key from index:%s";
  private final static String QUERY_REBUILD     = "rebuild index %s";
  private final static String QUERY_CLEAR       = "delete from index:%s";
  private final static String QUERY_DROP        = "drop index %s";
  protected final String           databaseName;
  private final   String           wrappedType;
  private final   String           algorithm;
  private final   ORID             rid;
  protected       OIndexDefinition indexDefinition;
  protected       String           name;
  protected       ODocument        configuration;
  protected       Set<String>      clustersToIndex;

  public OIndexRemote(final String iName, final String iWrappedType, final String algorithm, final ORID iRid,
      final OIndexDefinition iIndexDefinition, final ODocument iConfiguration, final Set<String> clustersToIndex, String database) {
    this.name = iName;
    this.wrappedType = iWrappedType;
    this.algorithm = algorithm;
    this.rid = iRid;
    this.indexDefinition = iIndexDefinition;
    this.configuration = iConfiguration;
    this.clustersToIndex = new HashSet<String>(clustersToIndex);
    this.databaseName = database;
  }

  public OIndexRemote<T> create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    this.name = name;
    return this;
  }

  public OIndexRemote<T> delete() {
    getDatabase().command(String.format(QUERY_DROP, name));
    return this;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public long getRebuildVersion() {
    throw new UnsupportedOperationException();
  }

  public boolean contains(final Object iKey) {
    try (final OResultSet result = getDatabase().command(String.format(QUERY_CONTAINS, name), iKey)) {
      if (!result.hasNext()) {
        return false;
      }
      return (Long) result.next().getProperty("size") > 0;
    }
  }

  public long count(final Object iKey) {
    try (final OResultSet result = getDatabase().command(String.format(QUERY_COUNT, name), iKey)) {
      if (!result.hasNext()) {
        return 0;
      }
      return (Long) result.next().getProperty("size");
    }
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

    try (OResultSet rs = getDatabase().command(query.toString(), iRangeFrom, iRangeTo)) {
      return (Long) rs.next().getProperty("value");
    }
  }

  public OIndexRemote<T> put(final Object iKey, final OIdentifiable iValue) {
    if (iValue instanceof ORecord && !iValue.getIdentity().isValid())
      // SAVE IT BEFORE TO PUT
      ((ORecord) iValue).save();

    if (iValue.getIdentity().isNew())
      throw new OIndexException(
          "Cannot insert values in manual indexes against remote protocol during a transaction. Temporary RID cannot be managed at server side");

    getDatabase().command(String.format(QUERY_PUT, name), iKey, iValue.getIdentity());
    return this;
  }

  public boolean remove(final Object key) {
    OResultSet result = getDatabase().command(String.format(QUERY_REMOVE, name), key);
    if (!result.hasNext()) {
      return false;
    }
    return ((long) result.next().getProperty("count")) > 0;
  }

  public boolean remove(final Object iKey, final OIdentifiable iRID) {
    final long deleted;
    if (iRID != null) {

      if (iRID.getIdentity().isNew())
        throw new OIndexException(
            "Cannot remove values in manual indexes against remote protocol during a transaction. Temporary RID cannot be managed at server side");

      OResultSet result = getDatabase().command(String.format(QUERY_REMOVE2, name), iKey, iRID);
      if (!result.hasNext()) {
        deleted = 0;
      } else
        deleted = result.next().getProperty("count");
      result.close();
    } else {
      OResultSet result = getDatabase().command(String.format(QUERY_REMOVE, name), iKey);
      if (!result.hasNext()) {
        deleted = 0;
      } else
        deleted = result.next().getProperty("count");
      result.close();
    }
    return deleted > 0;
  }

  public int remove(final OIdentifiable iRecord) {
    try (OResultSet rs = getDatabase().command(String.format(QUERY_REMOVE3, name, iRecord.getIdentity()), iRecord)) {
      return (Integer) rs.next().getProperty("value");
    }
  }

  @Override
  public int getVersion() {
    if (configuration == null)
      return -1;

    final Integer version = configuration.field(OIndexInternal.INDEX_VERSION);
    if (version != null)
      return version;

    return -1;
  }

  public void automaticRebuild() {
    throw new UnsupportedOperationException("autoRebuild()");
  }

  public long rebuild() {
    try (OResultSet rs = getDatabase().command(String.format(QUERY_REBUILD, name))) {
      return (Long) rs.next().getProperty("totalIndexed");
    }
  }

  public OIndexRemote<T> clear() {
    getDatabase().command(String.format(QUERY_CLEAR, name));
    return this;
  }

  public long getSize() {
    try (OResultSet result = getDatabase().query(String.format(QUERY_SIZE, name));) {
      if (result.hasNext())
        return (Long) result.next().getProperty("size");
    }
    return 0;
  }

  public long getKeySize() {
    try (OResultSet result = getDatabase().query(String.format(QUERY_KEY_SIZE, name))) {
      if (result.hasNext())
        return (Long) result.next().getProperty("size");
    }
    return 0;
  }

  public boolean isAutomatic() {
    return indexDefinition != null && indexDefinition.getClassName() != null;
  }

  @Override
  public boolean isUnique() {
    return false;
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

  public ORID getIdentity() {
    return rid;
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
    return new OType[0];
  }

  public Collection<ODocument> getEntries(final Collection<?> iKeys) {
    final StringBuilder params = new StringBuilder(128);
    if (!iKeys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < iKeys.size(); i++) {
        params.append(", ?");
      }
    }

    try (OResultSet rs = getDatabase().command(String.format(QUERY_GET_ENTRIES, name, params.toString()), iKeys.toArray())) {
      return rs.stream().map((res) -> (ODocument) res.toElement()).collect(Collectors.toList());
    }

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

  public Set<String> getClusters() {
    return Collections.unmodifiableSet(clustersToIndex);
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

    final StringBuilder params = new StringBuilder(128);
    if (!keys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < keys.size(); i++) {
        params.append(", ?");
      }
    }

    final OResultSet res = getDatabase()
        .command(String.format(QUERY_ITERATE_ENTRIES, name, params.toString(), ascSortOrder ? "ASC" : "DESC"), keys.toArray());

    final OInternalResultSet copy = new OInternalResultSet();//TODO a raw array instead...?
    res.forEachRemaining(x -> copy.add(x));
    res.close();

    return new OIndexAbstractCursor() {

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (!copy.hasNext())
          return null;
        final OResult next = copy.next();
        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return next.getProperty("key");
          }

          @Override
          public OIdentifiable getValue() {
            return next.getProperty("rid");
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException("cannot set value of index entry");
          }
        };
      }
    };

  }

  @Override
  public int getIndexId() {
    throw new UnsupportedOperationException("getIndexId");
  }

  @Override
  public OIndexCursor cursor() {
    OResultSet result = getDatabase().command(String.format(QUERY_ENTRIES, name));
    final OInternalResultSet copy = new OInternalResultSet();//TODO a raw array instead...?
    result.forEachRemaining(x -> copy.add(x));
    result.close();

    return new OIndexAbstractCursor() {

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (!copy.hasNext())
          return null;

        final OResult value = copy.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return value.getProperty("key");
          }

          @Override
          public OIdentifiable getValue() {
            return value.getProperty("rid");
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
    final OResultSet result = getDatabase().command(String.format(QUERY_ENTRIES_DESC, name));
    final OInternalResultSet copy = new OInternalResultSet();//TODO a raw array instead...?
    result.forEachRemaining(x -> copy.add(x));
    result.close();

    return new OIndexAbstractCursor() {

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (!copy.hasNext())
          return null;

        final OResult value = copy.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return value.getProperty("key");
          }

          @Override
          public OIdentifiable getValue() {
            return value.getProperty("rid");
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
    final OResultSet result = getDatabase().command(String.format(QUERY_KEYS, name));
    final OInternalResultSet copy = new OInternalResultSet();//TODO a raw array instead...?
    result.forEachRemaining(x -> copy.add(x));
    result.close();
    return new OIndexKeyCursor() {

      @Override
      public Object next(int prefetchSize) {
        if (!copy.hasNext())
          return null;

        final OResult value = copy.next();

        return value.getProperty("key");
      }
    };
  }

  @Override
  public int compareTo(OIndex<T> index) {
    final String name = index.getName();
    return this.name.compareTo(name);
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }
}
