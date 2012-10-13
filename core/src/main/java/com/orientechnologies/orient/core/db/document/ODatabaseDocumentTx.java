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
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordWrapperAbstract;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexMVRBTreeAbstract;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

@SuppressWarnings("unchecked")
public class ODatabaseDocumentTx extends ODatabaseRecordWrapperAbstract<ODatabaseRecordTx> implements ODatabaseDocument {
  public ODatabaseDocumentTx(final String iURL) {
    super(new ODatabaseRecordTx(iURL, ODocument.RECORD_TYPE));
  }

  public ODatabaseDocumentTx(final ODatabaseRecordTx iSource) {
    super(iSource);
  }

  private void freezeIndexes(final List<OIndexMVRBTreeAbstract<?>> indexesToFreeze, boolean throwException) {
    if (indexesToFreeze != null) {
      for (OIndexMVRBTreeAbstract<?> indexToLock : indexesToFreeze) {
        indexToLock.freeze(throwException);
      }
    }
  }

  private void flushIndexes(List<OIndexMVRBTreeAbstract<?>> indexesToFlush) {
    for (OIndexMVRBTreeAbstract<?> index : indexesToFlush) {
      index.flush();
    }
  }

  private List<OIndexMVRBTreeAbstract<?>> prepareIndexesToFreeze(Collection<? extends OIndex<?>> indexes) {
    List<OIndexMVRBTreeAbstract<?>> indexesToFreeze = null;
    if (indexes != null && !indexes.isEmpty()) {
      indexesToFreeze = new ArrayList<OIndexMVRBTreeAbstract<?>>(indexes.size());
      for (OIndex<?> index : indexes) {
        indexesToFreeze.add((OIndexMVRBTreeAbstract<?>) index.getInternal());
      }

      Collections.sort(indexesToFreeze, new Comparator<OIndex<?>>() {
        public int compare(OIndex<?> o1, OIndex<?> o2) {
          return o1.getName().compareTo(o2.getName());
        }
      });

    }
    return indexesToFreeze;
  }

  private void releaseIndexes(Collection<? extends OIndex<?>> indexesToRelease) {
    if (indexesToRelease != null) {
      Iterator<? extends OIndex<?>> it = indexesToRelease.iterator();
      while (it.hasNext()) {
        it.next().getInternal().release();
        it.remove();
      }
    }
  }

  @Override
  public void freeze(final boolean throwException) {
    if (!(getStorage() instanceof OStorageLocal)) {
      OLogManager.instance().error(this,
          "We can not freeze non local storage. " + "If you use remote client please use OServerAdmin instead.");

      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final Collection<? extends OIndex<?>> indexes = getMetadata().getIndexManager().getIndexes();
    final List<OIndexMVRBTreeAbstract<?>> indexesToLock = prepareIndexesToFreeze(indexes);

    freezeIndexes(indexesToLock, true);
    flushIndexes(indexesToLock);

    super.freeze(throwException);

    Orient.instance().getProfiler().stopChrono("document.database.freeze", "Time to freeze the database", startTime);
  }

  @Override
  public void freeze() {
    if (!(getStorage() instanceof OStorageLocal)) {
      OLogManager.instance().error(this,
          "We can not freeze non local storage. " + "If you use remote client please use OServerAdmin instead.");

      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final Collection<? extends OIndex<?>> indexes = getMetadata().getIndexManager().getIndexes();
    final List<OIndexMVRBTreeAbstract<?>> indexesToLock = prepareIndexesToFreeze(indexes);

    freezeIndexes(indexesToLock, false);
    flushIndexes(indexesToLock);

    super.freeze();

    Orient.instance().getProfiler().stopChrono("document.database.freeze", "Time to freeze the database", startTime);
  }

  @Override
  public void release() {
    if (!(getStorage() instanceof OStorageLocal)) {
      OLogManager.instance().error(this,
          "We can not release non local storage. " + "If you use remote client please use OServerAdmin instead.");
      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    super.release();

    Collection<? extends OIndex<?>> indexes = getMetadata().getIndexManager().getIndexes();
    releaseIndexes(indexes);

    Orient.instance().getProfiler().stopChrono("document.database.release", "Time to release the database", startTime);
  }

  /**
   * Creates a new ODocument.
   */
  @Override
  public ODocument newInstance() {
    return new ODocument();
  }

  public ODocument newInstance(final String iClassName) {
    checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, iClassName);
    return new ODocument(iClassName);
  }

  public ORecordIteratorClass<ODocument> browseClass(final String iClassName) {
    return browseClass(iClassName, true);
  }

  public ORecordIteratorClass<ODocument> browseClass(final String iClassName, final boolean iPolymorphic) {
    if (getMetadata().getSchema().getClass(iClassName) == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' not found in current database");

    checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, iClassName);

    return new ORecordIteratorClass<ODocument>(this, underlying, iClassName, iPolymorphic);
  }

  @Override
  public ORecordIteratorCluster<ODocument> browseCluster(final String iClusterName) {
    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(this, underlying, getClusterIdByName(iClusterName));
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
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iRecord) {
    return (RET) save(iRecord, OPERATION_MODE.SYNCHRONOUS, false, null);
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
   * @param iRecord
   *          Record to save.
   * @param iForceCreate
   *          Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @throws OConcurrentModificationException
   *           if the version of the document is different by the version contained in the database.
   * @throws OValidationException
   *           if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iCallback) {
    if (!(iRecord instanceof ODocument))
      return (RET) super.save(iRecord, iMode, iForceCreate, iCallback);

    ODocument doc = (ODocument) iRecord;
    doc.validate();
    doc.convertAllMultiValuesToTrackedVersions();

    try {
      if (iForceCreate || doc.getIdentity().isNew()) {
        // NEW RECORD
        if (doc.getClassName() != null)
          checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());

        if (doc.getSchemaClass() != null && doc.getIdentity().getClusterId() < 0) {
          // CLASS FOUND: FORCE THE STORING IN THE CLUSTER CONFIGURED
          String clusterName = getClusterNameById(doc.getSchemaClass().getDefaultClusterId());

          return (RET) super.save(doc, clusterName, iMode, iForceCreate, iCallback);
        }
      } else {
        // UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
        if (doc.getClassName() != null)
          checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_UPDATE, doc.getClassName());
      }

      doc = super.save(doc, iMode, iForceCreate, iCallback);

    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      OLogManager.instance().exception("Error on saving record %s of class '%s'", e, ODatabaseException.class,
          iRecord.getIdentity(), (doc.getClassName() != null ? doc.getClassName() : "?"));
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
   * @see #setMVCC(boolean), {@link #isMVCC()}, ORecordSchemaAware#validate()
   */
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iRecord, final String iClusterName) {
    return (RET) save(iRecord, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null);
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
   * @param iMode
   *          Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate
   *          Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @throws OConcurrentModificationException
   *           if the version of the document is different by the version contained in the database.
   * @throws OValidationException
   *           if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ORecordSchemaAware#validate()
   */
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iRecord, String iClusterName,
      final OPERATION_MODE iMode, boolean iForceCreate, final ORecordCallback<? extends Number> iCallback) {
    if (!(iRecord instanceof ODocument))
      return (RET) super.save(iRecord, iClusterName, iMode, iForceCreate, iCallback);

    ODocument doc = (ODocument) iRecord;

    if (iForceCreate || !doc.getIdentity().isValid()) {
      if (doc.getClassName() != null)
        checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());

      if (iClusterName == null && doc.getSchemaClass() != null)
        // FIND THE RIGHT CLUSTER AS CONFIGURED IN CLASS
        iClusterName = getClusterNameById(doc.getSchemaClass().getDefaultClusterId());

      int id = getClusterIdByName(iClusterName);
      if (id == -1)
        throw new IllegalArgumentException("Cluster name " + iClusterName + " is not configured");

      final int[] clusterIds;
      if (doc.getSchemaClass() != null) {
        // CHECK IF THE CLUSTER IS PART OF THE CONFIGURED CLUSTERS
        clusterIds = doc.getSchemaClass().getClusterIds();
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
    doc.convertAllMultiValuesToTrackedVersions();

    doc = super.save(doc, iClusterName, iMode, iForceCreate, iCallback);
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
   * @param iRecord
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  public ODatabaseDocumentTx delete(final ODocument iRecord) {
    if (iRecord == null)
      throw new ODatabaseException("Cannot delete null document");

    // CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
    if (iRecord.getClassName() != null)
      checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_DELETE, iRecord.getClassName());

    try {
      underlying.delete(iRecord);

    } catch (Exception e) {
      OLogManager.instance().exception("Error on deleting record %s of class '%s'", e, ODatabaseException.class,
          iRecord.getIdentity(), iRecord.getClassName());
    }
    return this;
  }

  /**
   * Returns the number of the records of the class iClassName.
   */
  public long countClass(final String iClassName) {
    final OClass cls = getMetadata().getSchema().getClass(iClassName);

    if (cls == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' not found in database");

    return cls.count();
  }

  public ODatabaseComplex<ORecordInternal<?>> commit() {
    try {
      return underlying.commit();
    } finally {
      getTransaction().close();
    }
  }

  public ODatabaseComplex<ORecordInternal<?>> rollback() {
    try {
      return underlying.rollback();
    } finally {
      getTransaction().close();
    }
  }

  public String getType() {
    return TYPE;
  }
}
