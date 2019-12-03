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
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Proxied abstract index.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OIndexRemote<T> implements OIndex {
  public static final  String QUERY_GET_VALUES_BEETWEN_SELECT                   = "select from index:`%s` where ";
  private static final String QUERY_GET_VALUES_BEETWEN_INCLUSIVE_FROM_CONDITION = "key >= ?";
  private static final String QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_FROM_CONDITION = "key > ?";
  private static final String QUERY_GET_VALUES_BEETWEN_INCLUSIVE_TO_CONDITION   = "key <= ?";
  private static final String QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_TO_CONDITION   = "key < ?";
  private static final String QUERY_GET_VALUES_AND_OPERATOR                     = " and ";
  private static final String QUERY_GET_VALUES_LIMIT                            = " limit ";
  private static final String QUERY_ENTRIES                                     = "select key, rid from index:`%s`";
  private static final String QUERY_ENTRIES_DESC                                = "select key, rid from index:`%s` order by key desc";

  private static final String QUERY_ITERATE_ENTRIES = "select from index:`%s` where key in [%s] order by key %s ";
  private static final String QUERY_GET_ENTRIES     = "select from index:`%s` where key in [%s]";

  private static final String           QUERY_PUT         = "insert into index:`%s` (key,rid) values (?,?)";
  private static final String           QUERY_REMOVE      = "delete from index:`%s` where key = ?";
  private static final String           QUERY_REMOVE2     = "delete from index:`%s` where key = ? and rid = ?";
  private static final String           QUERY_REMOVE3     = "delete from index:`%s` where rid = ?";
  private static final String           QUERY_CONTAINS    = "select count(*) as size from index:`%s` where key = ?";
  private static final String           QUERY_COUNT       = "select count(*) as size from index:`%s` where key = ?";
  private static final String           QUERY_COUNT_RANGE = "select count(*) as size from index:`%s` where ";
  private static final String           QUERY_SIZE        = "select count(*) as size from index:`%s`";
  private static final String           QUERY_KEY_SIZE    = "select indexKeySize('%s') as size";
  private static final String           QUERY_KEYS        = "select key from index:`%s`";
  private static final String           QUERY_REBUILD     = "rebuild index %s";
  private static final String           QUERY_CLEAR       = "delete from index:`%s`";
  private static final String           QUERY_DROP        = "drop index %s";
  protected final      String           databaseName;
  private final        String           wrappedType;
  private final        String           algorithm;
  private final        ORID             rid;
  protected            OIndexDefinition indexDefinition;
  protected            String           name;
  protected            ODocument        configuration;
  protected            Set<String>      clustersToIndex;

  public OIndexRemote(final String iName, final String iWrappedType, final String algorithm, final ORID iRid,
      final OIndexDefinition iIndexDefinition, final ODocument iConfiguration, final Set<String> clustersToIndex, String database) {
    this.name = iName;
    this.wrappedType = iWrappedType;
    this.algorithm = algorithm;
    this.rid = iRid;
    this.indexDefinition = iIndexDefinition;
    this.configuration = iConfiguration;
    this.clustersToIndex = new HashSet<>(clustersToIndex);
    this.databaseName = database;
  }

  public OIndexRemote<T> create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    this.name = name;
    return this;
  }

  public OIndexRemote<T> delete() {
    getDatabase().indexQuery(name, String.format(QUERY_DROP, name)).close();
    return this;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public long getRebuildVersion() {
    throw new UnsupportedOperationException();
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

    try (OResultSet rs = getDatabase().indexQuery(name, query.toString(), iRangeFrom, iRangeTo)) {
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

    getDatabase().command(String.format(QUERY_PUT, name), iKey, iValue.getIdentity()).close();
    return this;
  }

  public boolean remove(final Object key) {
    try (OResultSet result = getDatabase().command(String.format(QUERY_REMOVE, name), key)) {
      if (!result.hasNext()) {
        return false;
      }
      return ((long) result.next().getProperty("count")) > 0;
    }
  }

  public boolean remove(final Object iKey, final OIdentifiable iRID) {
    final long deleted;
    if (iRID != null) {

      if (iRID.getIdentity().isNew())
        throw new OIndexException(
            "Cannot remove values in manual indexes against remote protocol during a transaction. Temporary RID cannot be managed at server side");

      try (OResultSet result = getDatabase().command(String.format(QUERY_REMOVE2, name), iKey, iRID)) {
        if (!result.hasNext()) {
          deleted = 0;
        } else {
          deleted = result.next().getProperty("count");
        }
      }
    } else {
      try (OResultSet result = getDatabase().command(String.format(QUERY_REMOVE, name), iKey)) {
        if (!result.hasNext()) {
          deleted = 0;
        } else {
          deleted = result.next().getProperty("count");
        }
      }
    }
    return deleted > 0;
  }

  public int remove(final OIdentifiable record) {
    try (final OResultSet rs = getDatabase().command(String.format(QUERY_REMOVE3, name), record.getIdentity())) {
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

  public long rebuild() {
    try (OResultSet rs = getDatabase().command(String.format(QUERY_REBUILD, name))) {
      return (Long) rs.next().getProperty("totalIndexed");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public OIndexRemote<T> clear() {
    getDatabase().command(String.format(QUERY_CLEAR, name)).close();
    return this;
  }

  public long getSize() {
    try (OResultSet result = getDatabase().indexQuery(name, String.format(QUERY_SIZE, name))) {
      if (result.hasNext())
        return (Long) result.next().getProperty("size");
    }
    return 0;
  }

  public long getKeySize() {
    try (OResultSet result = getDatabase().indexQuery(name, String.format(QUERY_KEY_SIZE, name))) {
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

  public OIndexInternal getInternal() {
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

    try (OResultSet rs = getDatabase()
        .indexQuery(name, String.format(QUERY_GET_ENTRIES, name, params.toString()), iKeys.toArray())) {
      //noinspection resource
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
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesMinor");
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntries(Collection<?> keys, boolean ascSortOrder) {

    final StringBuilder params = new StringBuilder(128);
    if (!keys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < keys.size(); i++) {
        params.append(", ?");
      }
    }

    final OResultSet res = getDatabase()
        .indexQuery(name, String.format(QUERY_ITERATE_ENTRIES, name, params.toString(), ascSortOrder ? "ASC" : "DESC"),
            keys.toArray());

    return convertResultSetToIndexStream(res);
  }

  private static Stream<ORawPair<Object, ORID>> convertResultSetToIndexStream(OResultSet resultSet) {
    //noinspection resource
    return resultSet.stream().map((result) -> new ORawPair<>(result.getProperty("key"), result.getProperty("rid")));
  }

  @Override
  public int getIndexId() {
    throw new UnsupportedOperationException("getIndexId");
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream() {
    OResultSet res = getDatabase().indexQuery(name, String.format(QUERY_ENTRIES, name));
    return convertResultSetToIndexStream(res);

  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    final OResultSet result = getDatabase().indexQuery(name, String.format(QUERY_ENTRIES_DESC, name));
    return convertResultSetToIndexStream(result);
  }

  @Override
  public Stream<Object> keyStream() {
    @SuppressWarnings("resource")
    final OResultSet res = getDatabase().indexQuery(name, String.format(QUERY_KEYS, name));
    //noinspection resource
    return res.stream().map((result) -> result.getProperty("key"));
  }

  @Override
  public int compareTo(OIndex index) {
    final String name = index.getName();
    return this.name.compareTo(name);
  }

  protected static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }
}
