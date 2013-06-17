/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

/**
 * Proxied abstract index.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public abstract class OIndexRemote<T> implements OIndex<T> {
  private final String          wrappedType;
  private final ORID            rid;
  protected final String        databaseName;
  protected OIndexDefinition    indexDefinition;
  protected String              name;
  protected ODocument           configuration;
  protected Set<String>         clustersToIndex;

  protected final static String QUERY_ENTRIES                                     = "select key, rid from index:%s";

  private final static String   QUERY_GET_MAJOR                                   = "select from index:%s where key > ?";
  private final static String   QUERY_GET_MAJOR_EQUALS                            = "select from index:%s where key >= ?";
  private final static String   QUERY_GET_VALUE_MAJOR                             = "select EXPAND( rid ) from index:%s where key > ?";
  private final static String   QUERY_GET_VALUE_MAJOR_EQUALS                      = "select EXPAND( rid ) from index:%s where key >= ?";
  private final static String   QUERY_GET_MINOR                                   = "select from index:%s where key < ?";
  private final static String   QUERY_GET_MINOR_EQUALS                            = "select from index:%s where key <= ?";
  private final static String   QUERY_GET_VALUE_MINOR                             = "select EXPAND( rid ) from index:%s where key < ?";
  private final static String   QUERY_GET_VALUE_MINOR_EQUALS                      = "select EXPAND( rid ) from index:%s where key <= ?";
  private final static String   QUERY_GET_RANGE                                   = "select from index:%s where key between ? and ?";

  private final static String   QUERY_GET_VALUES                                  = "select EXPAND( rid ) from index:%s where key in [%s]";
  private final static String   QUERY_GET_ENTRIES                                 = "select from index:%s where key in [%s]";

  private final static String   QUERY_GET_VALUE_RANGE                             = "select EXPAND( rid ) from index:%s where key between ? and ?";

  private final static String   QUERY_PUT                                         = "insert into index:%s (key,rid) values (?,?)";
  private final static String   QUERY_REMOVE                                      = "delete from index:%s where key = ?";
  private final static String   QUERY_REMOVE2                                     = "delete from index:%s where key = ? and rid = ?";
  private final static String   QUERY_REMOVE3                                     = "delete from index:%s where rid = ?";
  private final static String   QUERY_CONTAINS                                    = "select count(*) as size from index:%s where key = ?";
  private final static String   QUERY_COUNT                                       = "select count(*) as size from index:%s where key = ?";
  private final static String   QUERY_SIZE                                        = "select count(*) as size from index:%s";
  private final static String   QUERY_KEY_SIZE                                    = "select count(distinct( key )) as size from index:%s";
  private final static String   QUERY_KEYS                                        = "select key from index:%s";
  private final static String   QUERY_REBUILD                                     = "rebuild index %s";
  private final static String   QUERY_CLEAR                                       = "delete from index:%s";

  public static final String    QUERY_GET_VALUES_BEETWEN_SELECT                   = "select from index:%s where ";
  public static final String    QUERY_GET_VALUES_BEETWEN_INCLUSIVE_FROM_CONDITION = "key >= ?";
  public static final String    QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_FROM_CONDITION = "key > ?";
  public static final String    QUERY_GET_VALUES_BEETWEN_INCLUSIVE_TO_CONDITION   = "key <= ?";
  public static final String    QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_TO_CONDITION   = "key < ?";
  public static final String    QUERY_GET_VALUES_AND_OPERATOR                     = " and ";
  public static final String    QUERY_GET_VALUES_LIMIT                            = " limit ";

  public OIndexRemote(final String iName, final String iWrappedType, final ORID iRid, final OIndexDefinition iIndexDefinition,
      final ODocument iConfiguration, final Set<String> clustersToIndex) {
    this.name = iName;
    this.wrappedType = iWrappedType;
    this.rid = iRid;
    this.indexDefinition = iIndexDefinition;
    this.configuration = iConfiguration;
    this.clustersToIndex = new HashSet<String>(clustersToIndex);
    this.databaseName = ODatabaseRecordThreadLocal.INSTANCE.get().getName();
  }

  public OIndexRemote<T> create(final String iName, final OIndexDefinition iIndexDefinition, final ODatabaseRecord iDatabase,
      final String iClusterIndexName, final int[] iClusterIdsToIndex, boolean rebuild, final OProgressListener iProgressListener) {
    name = iName;
    // final OCommandRequest cmd = formatCommand(QUERY_CREATE, name, wrappedType);
    // database.command(cmd).execute();
    return this;
  }

  public OIndexRemote<T> delete() {
    // final OCommandRequest cmd = formatCommand(QUERY_DROP, name);
    // database.command(cmd).execute();
    return this;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Set<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
    final OCommandRequest cmd = formatCommand(QUERY_GET_RANGE, name);
    return (Set<ODocument>) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
  }

  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo) {
    final OCommandRequest cmd = formatCommand(QUERY_GET_VALUE_RANGE, name);
    return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
  }

  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive) {
    final StringBuilder query = new StringBuilder(QUERY_GET_VALUES_BEETWEN_SELECT);

    if (iFromInclusive) {
      query.append(QUERY_GET_VALUES_BEETWEN_INCLUSIVE_FROM_CONDITION);
    } else {
      query.append(QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_FROM_CONDITION);
    }

    query.append(QUERY_GET_VALUES_AND_OPERATOR);

    if (iToInclusive) {
      query.append(QUERY_GET_VALUES_BEETWEN_INCLUSIVE_TO_CONDITION);
    } else {
      query.append(QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_TO_CONDITION);
    }

    final OCommandRequest cmd = formatCommand(query.toString());
    return getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
  }

  public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo) {
    final OCommandRequest cmd = formatCommand(QUERY_GET_RANGE, name);
    return (Collection<ODocument>) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
  }

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive) {
    final OCommandRequest cmd;
    if (isInclusive)
      cmd = formatCommand(QUERY_GET_VALUE_MAJOR_EQUALS, name);
    else
      cmd = formatCommand(QUERY_GET_VALUE_MAJOR, name);
    return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(fromKey);
  }

  public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive) {
    final OCommandRequest cmd;
    if (isInclusive)
      cmd = formatCommand(QUERY_GET_MAJOR_EQUALS, name);
    else
      cmd = formatCommand(QUERY_GET_MAJOR, name);
    return (Collection<ODocument>) getDatabase().command(cmd).execute(fromKey);
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive) {
    final OCommandRequest cmd;
    if (isInclusive)
      cmd = formatCommand(QUERY_GET_VALUE_MINOR_EQUALS, name);
    else
      cmd = formatCommand(QUERY_GET_VALUE_MINOR, name);
    return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(toKey);
  }

  public Collection<ODocument> getEntriesMinor(final Object toKey, final boolean isInclusive) {
    final OCommandRequest cmd;
    if (isInclusive)
      cmd = formatCommand(QUERY_GET_MINOR_EQUALS, name);
    else
      cmd = formatCommand(QUERY_GET_MINOR, name);
    return (Collection<ODocument>) getDatabase().command(cmd).execute(toKey);
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

  public OIndexRemote<T> put(final Object iKey, final OIdentifiable iValue) {
    if (iValue instanceof ORecord<?> && !iValue.getIdentity().isValid())
      // SAVE IT BEFORE TO PUT
      ((ORecord<?>) iValue).save();

    if (iValue.getIdentity().isNew())
      throw new OIndexException(
          "Cannot insert values in manual indexes against remote protocol during a transaction. Temporary RID cannot be managed at server side");

    final OCommandRequest cmd = formatCommand(QUERY_PUT, name);
    getDatabase().command(cmd).execute(iKey, iValue.getIdentity());
    return this;
  }

  public boolean remove(final Object iKey) {
    final OCommandRequest cmd = formatCommand(QUERY_REMOVE, name);
    return ((Integer) getDatabase().command(cmd).execute(iKey)) > 0;
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

  public Iterable<Object> keys() {
    final OCommandRequest cmd = formatCommand(QUERY_KEYS, name);
    return (Iterable<Object>) getDatabase().command(cmd).execute();
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

  public void unload() {
  }

  public boolean isAutomatic() {
    return indexDefinition != null && indexDefinition.getClassName() != null;
  }

  public String getName() {
    return name;
  }

  /**
   * Do nothing.
   */
  public OIndexRemote<T> lazySave() {
    return this;
  }

  public String getType() {
    return wrappedType;
  }

  public ODocument getConfiguration() {
    return configuration;
  }

  public ORID getIdentity() {
    return rid;
  }

  protected OCommandRequest formatCommand(final String iTemplate, final Object... iArgs) {
    final String text = String.format(iTemplate, iArgs);
    return new OCommandSQL(text);
  }

  public void commit(final ODocument iDocument) {
  }

  public OIndexInternal<T> getInternal() {
    return null;
  }

  protected ODatabaseComplex<ORecordInternal<?>> getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public long rebuild(final OProgressListener iProgressListener) {
    return rebuild();
  }

  public OType[] getKeyTypes() {
    if (indexDefinition != null)
      return indexDefinition.getTypes();
    return null;
  }

  public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
    final StringBuilder params = new StringBuilder();
    if (!iKeys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < iKeys.size(); i++) {
        params.append(", ?");
      }
    }

    final OCommandRequest cmd = formatCommand(QUERY_GET_VALUES, name, params.toString());
    return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(iKeys.toArray());
  }

  public Collection<ODocument> getEntries(final Collection<?> iKeys) {
    final StringBuilder params = new StringBuilder();
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

  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive, final int maxValuesToFetch) {
    if (maxValuesToFetch < 0)
      return getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive);

    final StringBuilder query = new StringBuilder(QUERY_GET_VALUES_BEETWEN_SELECT);

    if (iFromInclusive) {
      query.append(QUERY_GET_VALUES_BEETWEN_INCLUSIVE_FROM_CONDITION);
    } else {
      query.append(QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_FROM_CONDITION);
    }

    query.append(QUERY_GET_VALUES_AND_OPERATOR);

    if (iToInclusive) {
      query.append(QUERY_GET_VALUES_BEETWEN_INCLUSIVE_TO_CONDITION);
    } else {
      query.append(QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_TO_CONDITION);
    }

    query.append(QUERY_GET_VALUES_LIMIT).append(maxValuesToFetch);

    final OCommandRequest cmd = formatCommand(query.toString());
    return getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
  }

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive, final int maxValuesToFetch) {
    if (maxValuesToFetch < 0)
      return getValuesMajor(fromKey, isInclusive);

    final OCommandRequest cmd;
    if (isInclusive)
      cmd = formatCommand(QUERY_GET_VALUE_MAJOR_EQUALS + QUERY_GET_VALUES_LIMIT + maxValuesToFetch, name);
    else
      cmd = formatCommand(QUERY_GET_VALUE_MAJOR + QUERY_GET_VALUES_LIMIT + maxValuesToFetch, name);

    return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(fromKey);
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive, final int maxValuesToFetch) {

    if (maxValuesToFetch < 0)
      return getValuesMinor(toKey, isInclusive);

    final OCommandRequest cmd;

    if (isInclusive)
      cmd = formatCommand(QUERY_GET_VALUE_MINOR_EQUALS + QUERY_GET_VALUES_LIMIT + maxValuesToFetch, name);
    else
      cmd = formatCommand(QUERY_GET_VALUE_MINOR + QUERY_GET_VALUES_LIMIT + maxValuesToFetch, name);
    return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(toKey);
  }

  public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive, final int maxEntriesToFetch) {
    if (maxEntriesToFetch < 0)
      return getEntriesMajor(fromKey, isInclusive);

    final OCommandRequest cmd;
    if (isInclusive)
      cmd = formatCommand(QUERY_GET_MAJOR_EQUALS + QUERY_GET_VALUES_LIMIT + maxEntriesToFetch, name);
    else
      cmd = formatCommand(QUERY_GET_MAJOR + QUERY_GET_VALUES_LIMIT + maxEntriesToFetch, name);

    return (Collection<ODocument>) getDatabase().command(cmd).execute(fromKey);
  }

  public Collection<ODocument> getEntriesMinor(final Object toKey, final boolean isInclusive, final int maxEntriesToFetch) {
    if (maxEntriesToFetch < 0)
      return getEntriesMinor(toKey, isInclusive);

    final OCommandRequest cmd;
    if (isInclusive)
      cmd = formatCommand(QUERY_GET_MINOR_EQUALS + QUERY_GET_VALUES_LIMIT + maxEntriesToFetch, name);
    else
      cmd = formatCommand(QUERY_GET_MINOR + QUERY_GET_VALUES_LIMIT + maxEntriesToFetch, name);
    return (Collection<ODocument>) getDatabase().command(cmd).execute(toKey);
  }

  public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive,
      final int maxEntriesToFetch) {
    if (maxEntriesToFetch < 0)
      return getEntriesBetween(iRangeFrom, iRangeTo, iInclusive);

    final OCommandRequest cmd = formatCommand(QUERY_GET_RANGE + QUERY_GET_VALUES_LIMIT + maxEntriesToFetch, name);
    return (Set<ODocument>) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
  }

  public Collection<OIdentifiable> getValues(final Collection<?> iKeys, final int maxValuesToFetch) {
    if (maxValuesToFetch < 0)
      return getValues(iKeys);

    final StringBuilder params = new StringBuilder();
    if (!iKeys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < iKeys.size(); i++) {
        params.append(", ?");
      }
    }

    final OCommandRequest cmd = formatCommand(QUERY_GET_VALUES + QUERY_GET_VALUES_LIMIT + maxValuesToFetch, name, params.toString());
    return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(iKeys.toArray());
  }

  public Collection<ODocument> getEntries(final Collection<?> iKeys, final int maxEntriesToFetch) {
    if (maxEntriesToFetch < 0)
      return getEntries(iKeys);

    final StringBuilder params = new StringBuilder();
    if (!iKeys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < iKeys.size(); i++) {
        params.append(", ?");
      }
    }

    final OCommandRequest cmd = formatCommand(QUERY_GET_ENTRIES + QUERY_GET_VALUES_LIMIT + maxEntriesToFetch, name,
        params.toString());
    return (Collection<ODocument>) getDatabase().command(cmd).execute(iKeys.toArray());
  }

  public Set<String> getClusters() {
    return Collections.unmodifiableSet(clustersToIndex);
  }

  public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
  }

  @Override
  public boolean isRebuiding() {
    return false;
  }
}
