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

package com.orientechnologies.orient.core.db.document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseRecordWrapperAbstract;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;

@SuppressWarnings("unchecked")
public class ODatabaseDocumentTx extends ODatabaseRecordWrapperAbstract<ODatabaseRecordTx> implements ODatabaseDocumentInternal {
  protected static ORecordSerializer defaultSerializer;
  static {
    defaultSerializer = ORecordSerializerFactory.instance().getFormat(
        OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.getValueAsString());
    if (defaultSerializer == null)
      throw new ODatabaseException("Impossible to find serializer with name "
          + OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.getValueAsString());
  }

  /**
   * Creates a new connection to the database.
   * 
   * @param iURL
   *          of the database
   */
  public ODatabaseDocumentTx(final String iURL) {
    super(new ODatabaseRecordTx(iURL, ODocument.RECORD_TYPE));
    underlying.setSerializer(defaultSerializer);
  }

  /**
   * For internal usage. Creates a new instance with specific {@link ODatabaseRecordTx}.
   * 
   * @param iSource
   *          to wrap
   */
  public ODatabaseDocumentTx(final ODatabaseRecordTx iSource) {
    super(iSource);
  }

  /**
   * @return default serializer which is used to serialize documents. Default serializer is common for all database instances.
   */
  public static ORecordSerializer getDefaultSerializer() {
    return defaultSerializer;
  }

  /**
   * Sets default serializer. The default serializer is common for all database instances.
   * 
   * @param iDefaultSerializer
   *          new default serializer value
   */
  public static void setDefaultSerializer(ORecordSerializer iDefaultSerializer) {
    defaultSerializer = iDefaultSerializer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void freeze(final boolean throwException) {
    checkOpeness();
    if (!(getStorage() instanceof OFreezableStorage)) {
      OLogManager.instance().error(this,
          "We can not freeze non local storage. " + "If you use remote client please use OServerAdmin instead.");

      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final Collection<? extends OIndex<?>> indexes = getMetadata().getIndexManager().getIndexes();
    final List<OIndexAbstract<?>> indexesToLock = prepareIndexesToFreeze(indexes);

    freezeIndexes(indexesToLock, true);
    flushIndexes(indexesToLock);

    super.freeze(throwException);

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".freeze", "Time to freeze the database", startTime, "db.*.freeze");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void freeze() {
    checkOpeness();
    if (!(getStorage() instanceof OFreezableStorage)) {
      OLogManager.instance().error(this,
          "We can not freeze non local storage. " + "If you use remote client please use OServerAdmin instead.");

      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final Collection<? extends OIndex<?>> indexes = getMetadata().getIndexManager().getIndexes();
    final List<OIndexAbstract<?>> indexesToLock = prepareIndexesToFreeze(indexes);

    freezeIndexes(indexesToLock, false);
    flushIndexes(indexesToLock);

    super.freeze();

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".freeze", "Time to freeze the database", startTime, "db.*.freeze");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void release() {
    checkOpeness();
    if (!(getStorage() instanceof OFreezableStorage)) {
      OLogManager.instance().error(this,
          "We can not release non local storage. " + "If you use remote client please use OServerAdmin instead.");
      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    super.release();

    Collection<? extends OIndex<?>> indexes = getMetadata().getIndexManager().getIndexes();
    releaseIndexes(indexes);

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".release", "Time to release the database", startTime, "db.*.release");
  }

  /**
   * Creates a new ODocument.
   */
  @Override
  public ODocument newInstance() {
    return new ODocument();
  }

  /**
   * Creates a document with specific class.
   * 
   * @param iClassName
   *          the name of class that should be used as a class of created document.
   * @return new instance of document.
   */
  @Override
  public ODocument newInstance(final String iClassName) {
    return new ODocument(iClassName);
  }

  /**
   * {@inheritDoc}
   */
  public ORecordIteratorClass<ODocument> browseClass(final String iClassName) {
    return browseClass(iClassName, true);
  }

  /**
   * {@inheritDoc}
   */
  public ORecordIteratorClass<ODocument> browseClass(final String iClassName, final boolean iPolymorphic) {
    if (getMetadata().getImmutableSchemaSnapshot().getClass(iClassName) == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' not found in current database");

    checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, iClassName);

    return new ORecordIteratorClass<ODocument>(this, underlying, iClassName, iPolymorphic, true, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ORecordIteratorCluster<ODocument> browseCluster(final String iClusterName) {
    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(this, underlying, getClusterIdByName(iClusterName), true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ORecordIteratorCluster<ODocument> browseCluster(String iClusterName, long startClusterPosition, long endClusterPosition,
      boolean loadTombstones) {
    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(this, underlying, getClusterIdByName(iClusterName), startClusterPosition,
        endClusterPosition, true, loadTombstones, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  /**
   * Saves a document to the database. Behavior depends by the current running transaction if any. If no transaction is running then
   * changes apply immediately. If an Optimistic transaction is running then the record will be changed at commit time. The current
   * transaction will continue to see the record as modified, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as modified, while others
   * cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   * 
   * @param iRecord
   *          Record to save.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @throws OConcurrentModificationException
   *           if the version of the document is different by the version contained in the database.
   * @throws OValidationException
   *           if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord) {
    return (RET) save(iRecord, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves a document to the database. Behavior depends by the current running transaction if any. If no transaction is running then
   * changes apply immediately. If an Optimistic transaction is running then the record will be changed at commit time. The current
   * transaction will continue to see the record as modified, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as modified, while others
   * cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   * 
   * 
   * 
   * @param iRecord
   *          Record to save.
   * @param iForceCreate
   *          Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @param iRecordCreatedCallback
   *          callback that is called after creation of new record
   * @param iRecordUpdatedCallback
   *          callback that is called after record update
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @throws OConcurrentModificationException
   *           if the version of the document is different by the version contained in the database.
   * @throws OValidationException
   *           if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    checkOpeness();
    if (!(iRecord instanceof ODocument))
      return (RET) super.save(iRecord, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);

    ODocument doc = (ODocument) iRecord;
    doc.validate();
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);

    try {
      if (iForceCreate || doc.getIdentity().isNew()) {
        // NEW RECORD
        if (doc.getClassName() != null)
          checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());

        final OClass schemaClass = doc.getImmutableSchemaClass();

        if (schemaClass != null && doc.getIdentity().getClusterId() < 0) {
          // CLASS FOUND: FORCE THE STORING IN THE CLUSTER CONFIGURED
          final String clusterName = getClusterNameById(doc.getImmutableSchemaClass().getClusterForNewInstance(doc));

          return (RET) super.save(doc, clusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
        }
      } else {
        // UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
        if (doc.getClassName() != null)
          checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_UPDATE, doc.getClassName());
      }

      doc = super.save(doc, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);

    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw new ODatabaseException("Error on saving record " + iRecord.getIdentity() + " of class  '"
          + (doc.getClassName() != null ? doc.getClassName() : "?") + "'", e);
    }
    return (RET) doc;
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the current running transaction if any. If
   * no transaction is running then changes apply immediately. If an Optimistic transaction is running then the record will be
   * changed at commit time. The current transaction will continue to see the record as modified, while others not. If a Pessimistic
   * transaction is running, then an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown. Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   * 
   * @param iRecord
   *          Record to save
   * @param iClusterName
   *          Cluster name where to save the record
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @throws OConcurrentModificationException
   *           if the version of the document is different by the version contained in the database.
   * @throws OValidationException
   *           if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ODocument#validate()
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, final String iClusterName) {
    return (RET) save(iRecord, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the current running transaction if any. If
   * no transaction is running then changes apply immediately. If an Optimistic transaction is running then the record will be
   * changed at commit time. The current transaction will continue to see the record as modified, while others not. If a Pessimistic
   * transaction is running, then an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown. Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   * 
   * 
   * @param iRecord
   *          Record to save
   * @param iClusterName
   *          Cluster name where to save the record
   * @param iMode
   *          Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate
   *          Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @param iRecordCreatedCallback
   *          callback that is called after creation of new record
   * @param iRecordUpdatedCallback
   *          callback that is called after record update
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @throws OConcurrentModificationException
   *           if the version of the document is different by the version contained in the database.
   * @throws OValidationException
   *           if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ODocument#validate()
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, String iClusterName, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    checkOpeness();
    if (!(iRecord instanceof ODocument))
      return (RET) super.save(iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);

    ODocument doc = (ODocument) iRecord;

    if (iForceCreate || !doc.getIdentity().isValid()) {
      if (doc.getClassName() != null)
        checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());

      final OClass schemaClass = doc.getImmutableSchemaClass();

      if (iClusterName == null && schemaClass != null)
        // FIND THE RIGHT CLUSTER AS CONFIGURED IN CLASS
        iClusterName = getClusterNameById(schemaClass.getClusterForNewInstance(doc));

      int id = getClusterIdByName(iClusterName);
      if (id == -1)
        throw new IllegalArgumentException("Cluster name " + iClusterName + " is not configured");

      final int[] clusterIds;
      if (schemaClass != null) {
        // CHECK IF THE CLUSTER IS PART OF THE CONFIGURED CLUSTERS
        clusterIds = schemaClass.getClusterIds();
        int i = 0;
        for (; i < clusterIds.length; ++i)
          if (clusterIds[i] == id)
            break;

        if (i == clusterIds.length)
          throw new IllegalArgumentException("Cluster name " + iClusterName + " is not configured to store the class "
              + doc.getClassName());
      } else
        clusterIds = new int[] { id };

    } else {
      // UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
      if (doc.getClassName() != null)
        checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_UPDATE, doc.getClassName());
    }

    doc.validate();
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);

    doc = super.save(doc, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
    return (RET) doc;
  }

  /**
   * Deletes a document. Behavior depends by the current running transaction if any. If no transaction is running then the record is
   * deleted immediately. If an Optimistic transaction is running then the record will be deleted at commit time. The current
   * transaction will continue to see the record as deleted, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as deleted, while others
   * cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.
   * 
   * @param record
   *          record to delete
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  public ODatabaseDocumentTx delete(final ORecord record) {
    checkOpeness();
    if (record == null)
      throw new ODatabaseException("Cannot delete null document");

    // CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
    if (record instanceof ODocument && ((ODocument) record).getClassName() != null)
      checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_DELETE, ((ODocument) record).getClassName());

    try {
      underlying.delete(record);

    } catch (Exception e) {
      if (record instanceof ODocument)
        throw new ODatabaseException("Error on deleting record " + record.getIdentity() + " of class '"
            + ((ODocument) record).getClassName() + "'", e);
      else
        throw new ODatabaseException("Error on deleting record " + record.getIdentity());
    }
    return this;
  }

  /**
   * Returns the number of the records of the class iClassName.
   */
  public long countClass(final String iClassName) {
    return countClass(iClassName, true);
  }

  /**
   * Returns the number of the records of the class iClassName considering also sub classes if polymorphic is true.
   */
  public long countClass(final String iClassName, final boolean iPolymorphic) {
    ODatabaseRecordThreadLocal.INSTANCE.set(this);

    final OClass cls = getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (cls == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' not found in database");

    return cls.count(iPolymorphic);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabaseComplex<ORecord> commit() {
    return commit(false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabaseComplex<ORecord> commit(boolean force) throws OTransactionException {
    return underlying.commit(force);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabaseComplex<ORecord> rollback() {
    return rollback(false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabaseComplex<ORecord> rollback(final boolean force) throws OTransactionException {
    return underlying.rollback(force);
  }

  /**
   * Returns "document".
   */
  public String getType() {
    return TYPE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OSBTreeCollectionManager getSbTreeCollectionManager() {
    return underlying.getSbTreeCollectionManager();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OCurrentStorageComponentsFactory getStorageVersions() {
    return underlying.getStorageVersions();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ORecordSerializer getSerializer() {
    return underlying.getSerializer();
  }

  /**
   * Sets serializer for the database which will be used for document serialization.
   * 
   * @param iSerializer
   *          the serializer to set.
   */
  public void setSerializer(final ORecordSerializer iSerializer) {
    underlying.setSerializer(iSerializer);
  }

  private void freezeIndexes(final List<OIndexAbstract<?>> indexesToFreeze, final boolean throwException) {
    if (indexesToFreeze != null) {
      for (OIndexAbstract<?> indexToLock : indexesToFreeze) {
        indexToLock.freeze(throwException);
      }
    }
  }

  private void flushIndexes(final List<OIndexAbstract<?>> indexesToFlush) {
    for (OIndexAbstract<?> index : indexesToFlush) {
      index.flush();
    }
  }

  private List<OIndexAbstract<?>> prepareIndexesToFreeze(final Collection<? extends OIndex<?>> indexes) {
    List<OIndexAbstract<?>> indexesToFreeze = null;
    if (indexes != null && !indexes.isEmpty()) {
      indexesToFreeze = new ArrayList<OIndexAbstract<?>>(indexes.size());
      for (OIndex<?> index : indexes) {
        indexesToFreeze.add((OIndexAbstract<?>) index.getInternal());
      }

      Collections.sort(indexesToFreeze, new Comparator<OIndex<?>>() {
        public int compare(OIndex<?> o1, OIndex<?> o2) {
          return o1.getName().compareTo(o2.getName());
        }
      });

    }
    return indexesToFreeze;
  }

  private void releaseIndexes(final Collection<? extends OIndex<?>> indexesToRelease) {
    if (indexesToRelease != null) {
      Iterator<? extends OIndex<?>> it = indexesToRelease.iterator();
      while (it.hasNext()) {
        it.next().getInternal().release();
        it.remove();
      }
    }
  }

  @Override
  public void resetInitialization() {
    underlying.resetInitialization();
  }
}
