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

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OPropertyAccess;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityResourceProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Interface to handle index.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OIndexInternal extends OIndex {

  String CONFIG_KEYTYPE = "keyType";
  String CONFIG_AUTOMATIC = "automatic";
  String CONFIG_TYPE = "type";
  String ALGORITHM = "algorithm";
  String VALUE_CONTAINER_ALGORITHM = "valueContainerAlgorithm";
  String CONFIG_NAME = "name";
  String INDEX_DEFINITION = "indexDefinition";
  String INDEX_DEFINITION_CLASS = "indexDefinitionClass";
  String INDEX_VERSION = "indexVersion";
  String METADATA = "metadata";

  Object getCollatingValue(final Object key);

  /**
   * Loads the index giving the configuration.
   *
   * @param iConfig ODocument instance containing the configuration
   */
  boolean loadFromConfiguration(ODocument iConfig);

  /**
   * Saves the index configuration to disk.
   *
   * @return The configuration as ODocument instance
   * @see #getConfiguration()
   */
  ODocument updateConfiguration();

  /**
   * Add given cluster to the list of clusters that should be automatically indexed.
   *
   * @param iClusterName Cluster to add.
   * @return Current index instance.
   */
  OIndex addCluster(final String iClusterName);

  /**
   * Remove given cluster from the list of clusters that should be automatically indexed.
   *
   * @param iClusterName Cluster to remove.
   * @return Current index instance.
   */
  OIndex removeCluster(final String iClusterName);

  /**
   * Indicates whether given index can be used to calculate result of {@link
   * com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   *
   * @return {@code true} if given index can be used to calculate result of {@link
   *     com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   */
  boolean canBeUsedInEqualityOperators();

  boolean hasRangeQuerySupport();

  OIndexMetadata loadMetadata(ODocument iConfig);

  void close();

  void preCommit(OIndexAbstract.IndexTxSnapshot snapshots);

  void addTxOperation(
      OIndexAbstract.IndexTxSnapshot snapshots, final OTransactionIndexChanges changes);

  void commit(OIndexAbstract.IndexTxSnapshot snapshots);

  void postCommit(OIndexAbstract.IndexTxSnapshot snapshots);

  void setType(OType type);

  /**
   * Returns the index name for a key. The name is always the current index name, but in cases where
   * the index supports key-based sharding.
   *
   * @param key the index key.
   * @return The index name involved
   */
  String getIndexNameByKey(Object key);

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * index.
   *
   * <p>If this index supports a more narrow locking, for example key-based sharding, it may use the
   * provided {@code key} to infer a more narrow lock scope, but that is not a requirement.
   *
   * @param key the index key to lock.
   * @return {@code true} if this index was locked entirely, {@code false} if this index locking is
   *     sensitive to the provided {@code key} and only some subset of this index was locked.
   */
  boolean acquireAtomicExclusiveLock(Object key);

  /** @return number of entries in the index. */
  long size();

  Stream<ORID> getRids(final Object key);

  Stream<ORawPair<Object, ORID>> stream();

  Stream<ORawPair<Object, ORID>> descStream();

  Stream<Object> keyStream();

  /**
   * Returns stream which presents subset of index data between passed in keys.
   *
   * @param fromKey Lower border of index data.
   * @param fromInclusive Indicates whether lower border should be inclusive or exclusive.
   * @param toKey Upper border of index data.
   * @param toInclusive Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder Flag which determines whether data iterated by stream should be in ascending or
   *     descending order.
   * @return Cursor which presents subset of index data between passed in keys.
   */
  Stream<ORawPair<Object, ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder);

  /**
   * Returns stream which presents data associated with passed in keys.
   *
   * @param keys Keys data of which should be returned.
   * @param ascSortOrder Flag which determines whether data iterated by stream should be in
   *     ascending or descending order.
   * @return stream which presents data associated with passed in keys.
   */
  Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder);

  /**
   * Returns stream which presents subset of data which associated with key which is greater than
   * passed in key.
   *
   * @param fromKey Lower border of index data.
   * @param fromInclusive Indicates whether lower border should be inclusive or exclusive.
   * @param ascOrder Flag which determines whether data iterated by stream should be in ascending or
   *     descending order.
   * @return stream which presents subset of data which associated with key which is greater than
   *     passed in key.
   */
  Stream<ORawPair<Object, ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder);

  /**
   * Returns stream which presents subset of data which associated with key which is less than
   * passed in key.
   *
   * @param toKey Upper border of index data.
   * @param toInclusive Indicates Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder Flag which determines whether data iterated by stream should be in ascending or
   *     descending order.
   * @return stream which presents subset of data which associated with key which is less than
   *     passed in key.
   */
  Stream<ORawPair<Object, ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder);

  static OIdentifiable securityFilterOnRead(OIndex idx, OIdentifiable item) {
    if (idx.getDefinition() == null) {
      return item;
    }
    String indexClass = idx.getDefinition().getClassName();
    if (indexClass == null) {
      return item;
    }
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return item;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    if (isReadRestrictedBySecurityPolicy(indexClass, db, security)) {
      item = item.getRecord();
    }
    if (item == null) {
      return null;
    }
    if (idx.getDefinition().getFields().size() == 1) {
      String indexProp = idx.getDefinition().getFields().get(0);
      if (isLabelSecurityDefined(db, security, indexClass, indexProp)) {
        item = item.getRecord();
        if (item == null) {
          return null;
        }
        if (!(item instanceof ODocument)) {
          return item;
        }
        OPropertyAccess access = ODocumentInternal.getPropertyAccess((ODocument) item);
        if (access != null && !access.isReadable(indexProp)) {
          return null;
        }
      }
    }
    return item;
  }

  static boolean isLabelSecurityDefined(
      ODatabaseDocumentInternal database,
      OSecurityInternal security,
      String indexClass,
      String propertyName) {
    Set<String> classesToCheck = new HashSet<>();
    classesToCheck.add(indexClass);
    OClass clazz = database.getClass(indexClass);
    if (clazz == null) {
      return false;
    }
    clazz.getAllSubclasses().forEach(x -> classesToCheck.add(x.getName()));
    clazz.getAllSuperClasses().forEach(x -> classesToCheck.add(x.getName()));
    Set<OSecurityResourceProperty> allFilteredProperties =
        security.getAllFilteredProperties(database);

    for (String className : classesToCheck) {
      Optional<OSecurityResourceProperty> item =
          allFilteredProperties.stream()
              .filter(x -> x.getClassName().equalsIgnoreCase(className))
              .filter(x -> x.getPropertyName().equals(propertyName))
              .findFirst();

      if (item.isPresent()) {
        return true;
      }
    }
    return false;
  }

  static boolean isReadRestrictedBySecurityPolicy(
      String indexClass, ODatabaseDocumentInternal db, OSecurityInternal security) {
    if (security.isReadRestrictedBySecurityPolicy(db, "database.class." + indexClass)) {
      return true;
    }

    OClass clazz = db.getClass(indexClass);
    if (clazz != null) {
      Collection<OClass> sub = clazz.getSubclasses();
      for (OClass subClass : sub) {
        if (isReadRestrictedBySecurityPolicy(subClass.getName(), db, security)) {
          return true;
        }
      }
    }

    return false;
  }

  static Collection securityFilterOnRead(OIndex idx, Collection<OIdentifiable> items) {
    if (idx.getMetadata() == null && idx.getDefinition() == null) {
      return items;
    }
    String indexClass =
        idx.getMetadata() == null
            ? idx.getDefinition().getClassName()
            : idx.getMetadata().getClassName();
    if (indexClass == null) {
      return items;
    }
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return items;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    if (isReadRestrictedBySecurityPolicy(indexClass, db, security)) {
      items =
          items.stream()
              .map(x -> x.getRecord()) // force record load, that triggers security checks
              .filter(x -> x != null)
              .map(x -> ((ORecord) x).getIdentity())
              .collect(Collectors.toList());
    }

    if (idx.getDefinition().getFields().size() == 1) {
      String indexProp = idx.getDefinition().getFields().get(0);
      if (isLabelSecurityDefined(db, security, indexClass, indexProp)) {

        items =
            items.stream()
                .map(x -> x.getRecord())
                .filter(x -> x != null)
                .filter(
                    x ->
                        !(x instanceof ODocument)
                            || ODocumentInternal.getPropertyAccess((ODocument) x) == null
                            || ODocumentInternal.getPropertyAccess((ODocument) x)
                                .isReadable(indexProp))
                .map(x -> ((ORecord) x).getIdentity())
                .collect(Collectors.toList());
      }
    }
    return items;
  }

  boolean isNativeTxSupported();

  Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes);

  void doPut(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException;

  boolean doRemove(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException;

  boolean doRemove(OAbstractPaginatedStorage storage, Object key)
      throws OInvalidIndexEngineIdException;
}
