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

package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionBlockedException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Delegates all the CRUD operations to the current transaction.
 * 
 */
public class ODatabaseRecordTx extends ODatabaseRecordAbstract {
  public static final String TYPE = "record";
  private OTransaction       currentTx;

  public ODatabaseRecordTx(final String iURL, final byte iRecordType) {
    super(iURL, iRecordType);
    init();
  }

  public ODatabaseRecord begin() {
    return begin(TXTYPE.OPTIMISTIC);
  }

  public ODatabaseRecord begin(final TXTYPE iType) {
    checkOpeness();
    setCurrentDatabaseinThreadLocal();

    if (currentTx.isActive()) {
      if (iType == TXTYPE.OPTIMISTIC && currentTx instanceof OTransactionOptimistic) {
        currentTx.begin();
        return this;
      }

      currentTx.rollback(true, 0);
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxBegin(this);
      } catch (Throwable t) {
        OLogManager.instance().error(this, "Error before tx begin", t);
      }

    switch (iType) {
    case NOTX:
      setDefaultTransactionMode();
      break;

    case OPTIMISTIC:
      currentTx = new OTransactionOptimistic(this);
      break;

    case PESSIMISTIC:
      throw new UnsupportedOperationException("Pessimistic transaction");
    }

    currentTx.begin();
    return this;
  }

  public ODatabaseRecord begin(final OTransaction iTx) {
    checkOpeness();
    if (currentTx.isActive() && iTx.equals(currentTx)) {
      currentTx.begin();
      return this;
    }

    currentTx.rollback(true, 0);

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxBegin(this);
      } catch (Throwable t) {
        OLogManager.instance().error(this, "Error before the transaction begin", t, OTransactionBlockedException.class);
      }

    currentTx = iTx;
    currentTx.begin();

    return this;
  }

  public ODatabaseRecord commit() {
    return commit(false);
  }

  @Override
  public ODatabaseRecord commit(boolean force) throws OTransactionException {
    checkOpeness();
    if (!currentTx.isActive())
      return this;

    if (!force && currentTx.amountOfNestedTxs() > 1) {
      currentTx.commit();
      return this;
    }

    setCurrentDatabaseinThreadLocal();
    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxCommit(this);
      } catch (Throwable t) {
        try {
          rollback(force);
        } catch (RuntimeException e) {
          throw e;
        }
        OLogManager.instance().debug(this, "Cannot commit the transaction: caught exception on execution of %s.onBeforeTxCommit()",
            t, OTransactionBlockedException.class, listener.getClass());
      }

    try {
      currentTx.commit(force);
    } catch (RuntimeException e) {
      // WAKE UP ROLLBACK LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onBeforeTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error before tx rollback", t);
        }
      // ROLLBACK TX AT DB LEVEL
      currentTx.rollback(false, 0);
      getLocalCache().clear();

      // WAKE UP ROLLBACK LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onAfterTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error after tx rollback", t);
        }
      throw e;
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onAfterTxCommit(this);
      } catch (Throwable t) {
        OLogManager
            .instance()
            .debug(
                this,
                "Error after the transaction has been committed. The transaction remains valid. The exception caught was on execution of %s.onAfterTxCommit()",
                t, OTransactionBlockedException.class, listener.getClass());
      }

    return this;
  }

  @Override
  public OContextConfiguration getConfiguration() {
    if (storage != null)
      return storage.getConfiguration().getContextConfiguration();
    return null;
  }

  @Override
  public boolean declareIntent(OIntent iIntent) {
    if (currentIntent != null) {
      if (iIntent != null && iIntent.getClass().equals(currentIntent.getClass()))
        // SAME INTENT: JUMP IT
        return false;

      // END CURRENT INTENT
      currentIntent.end(this);
    }

    currentIntent = iIntent;

    if (iIntent != null)
      iIntent.begin(this);

    return true;
  }

  @Override
  public boolean exists() {
    if (status == STATUS.OPEN)
      return true;

    if (storage == null)
      storage = Orient.instance().loadStorage(url);

    return storage.exists();
  }

  @Override
  public void close() {
    if (isClosed())
      return;

    try {
      commit(true);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during commit of active transaction.", e);
    }

    super.close();
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public long getSize() {
    return storage.getSize();
  }

  @Override
  public String getName() {
    return storage != null ? storage.getName() : url;
  }

  @Override
  public String getURL() {
    return url != null ? url : storage.getURL();
  }

  @Override
  public int getDefaultClusterId() {
    return storage.getDefaultClusterId();
  }

  @Override
  public int getClusters() {
    return storage.getClusters();
  }

  @Override
  public boolean existsCluster(String iClusterName) {
    return storage.getClusterNames().contains(iClusterName.toLowerCase());
  }

  @Override
  public Collection<String> getClusterNames() {
    return storage.getClusterNames();
  }

  @Override
  public int getClusterIdByName(String iClusterName) {
    return storage.getClusterIdByName(iClusterName.toLowerCase());
  }

  @Override
  public String getClusterNameById(int iClusterId) {
    if (iClusterId == -1)
      return null;

    // PHIYSICAL CLUSTER
    return storage.getPhysicalClusterNameById(iClusterId);
  }

  @Override
  public long getClusterRecordSizeByName(String clusterName) {
    try {
      return storage.getClusterById(getClusterIdByName(clusterName)).getRecordsSize();
    } catch (Exception e) {
      throw new ODatabaseException("Error on reading records size for cluster '" + clusterName + "'", e);
    }
  }

  @Override
  public long getClusterRecordSizeById(int clusterId) {
    try {
      return storage.getClusterById(clusterId).getRecordsSize();
    } catch (Exception e) {
      throw new ODatabaseException("Error on reading records size for cluster with id '" + clusterId + "'", e);
    }
  }

  @Override
  public boolean isClosed() {
    return status == STATUS.CLOSED || storage.isClosed();
  }

  @Override
  public int addCluster(String iClusterName, Object... iParameters) {
    return storage.addCluster(iClusterName, false, iParameters);
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId, Object... iParameters) {
    return storage.addCluster(iClusterName, iRequestedId, false, iParameters);
  }

  @Override
  public boolean dropCluster(String iClusterName, boolean iTruncate) {
    return storage.dropCluster(iClusterName, iTruncate);
  }

  @Override
  public boolean dropCluster(int iClusterId, boolean iTruncate) {
    return storage.dropCluster(iClusterId, iTruncate);
  }

  @Override
  public Object setProperty(String iName, Object iValue) {
    if (iValue == null)
      return properties.remove(iName.toLowerCase());
    else
      return properties.put(iName.toLowerCase(), iValue);
  }

  @Override
  public Object getProperty(String iName) {
    return properties.get(iName.toLowerCase());
  }

  @Override
  public Iterator<Map.Entry<String, Object>> getProperties() {
    return properties.entrySet().iterator();
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    switch (iAttribute) {
    case STATUS:
      return getStatus();
    case DEFAULTCLUSTERID:
      return getDefaultClusterId();
    case TYPE:
      return getMetadata().getImmutableSchemaSnapshot().existsClass("V") ? "graph" : "document";
    case DATEFORMAT:
      return storage.getConfiguration().dateFormat;

    case DATETIMEFORMAT:
      return storage.getConfiguration().dateTimeFormat;

    case TIMEZONE:
      return storage.getConfiguration().getTimeZone().getID();

    case LOCALECOUNTRY:
      return storage.getConfiguration().getLocaleCountry();

    case LOCALELANGUAGE:
      return storage.getConfiguration().getLocaleLanguage();

    case CHARSET:
      return storage.getConfiguration().getCharset();

    case CUSTOM:
      return storage.getConfiguration().properties;

    case CLUSTERSELECTION:
      return storage.getConfiguration().getClusterSelection();

    case MINIMUMCLUSTERS:
      return storage.getConfiguration().getMinimumClusters();

    case CONFLICTSTRATEGY:
      return storage.getConfiguration().getConflictStrategy();
    }

    return null;
  }

  @Override
  public <DB extends ODatabase> DB set(ATTRIBUTES iAttribute, Object iValue) {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = OStringSerializerHelper.getStringContent(iValue != null ? iValue.toString() : null);

    switch (iAttribute) {
    case STATUS:
      if (stringValue == null)
        throw new IllegalArgumentException("DB status can't be null");
      setStatus(STATUS.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
      break;

    case DEFAULTCLUSTERID:
      if (iValue != null) {
        if (iValue instanceof Number)
          storage.setDefaultClusterId(((Number) iValue).intValue());
        else
          storage.setDefaultClusterId(storage.getClusterIdByName(iValue.toString()));
      }
      break;

    case TYPE:
      throw new IllegalArgumentException("Database type property is not supported");

    case DATEFORMAT:
      // CHECK FORMAT
      new SimpleDateFormat(stringValue).format(new Date());

      storage.getConfiguration().dateFormat = stringValue;
      storage.getConfiguration().update();
      break;

    case DATETIMEFORMAT:
      // CHECK FORMAT
      new SimpleDateFormat(stringValue).format(new Date());

      storage.getConfiguration().dateTimeFormat = stringValue;
      storage.getConfiguration().update();
      break;

    case TIMEZONE:
      if (stringValue == null)
        throw new IllegalArgumentException("Timezone can't be null");

      storage.getConfiguration().setTimeZone(TimeZone.getTimeZone(stringValue.toUpperCase()));
      storage.getConfiguration().update();
      break;

    case LOCALECOUNTRY:
      storage.getConfiguration().setLocaleCountry(stringValue);
      storage.getConfiguration().update();
      break;

    case LOCALELANGUAGE:
      storage.getConfiguration().setLocaleLanguage(stringValue);
      storage.getConfiguration().update();
      break;

    case CHARSET:
      storage.getConfiguration().setCharset(stringValue);
      storage.getConfiguration().update();
      break;

    case CUSTOM:
      int indx = stringValue != null ? stringValue.indexOf('=') : -1;
      if (indx < 0) {
        if ("clear".equalsIgnoreCase(stringValue)) {
          clearCustomInternal();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        String customName = stringValue.substring(0, indx).trim();
        String customValue = stringValue.substring(indx + 1).trim();
        if (customValue.isEmpty())
          removeCustomInternal(customName);
        else
          setCustomInternal(customName, customValue);
      }
      break;

    case CLUSTERSELECTION:
      storage.getConfiguration().setClusterSelection(stringValue);
      storage.getConfiguration().update();
      break;

    case MINIMUMCLUSTERS:
      if (iValue != null) {
        if (iValue instanceof Number)
          storage.getConfiguration().setMinimumClusters(((Number) iValue).intValue());
        else
          storage.getConfiguration().setMinimumClusters(Integer.parseInt(stringValue));
      } else
        // DEFAULT = 1
        storage.getConfiguration().setMinimumClusters(1);

      storage.getConfiguration().update();
      break;

    case CONFLICTSTRATEGY:
      storage.getConfiguration().setConflictStrategy(stringValue);
      storage.getConfiguration().update();
      break;

    default:
      throw new IllegalArgumentException("Option '" + iAttribute + "' not supported on alter database");

    }

    return (DB) this;
  }

  private void clearCustomInternal() {
    storage.getConfiguration().properties.clear();
  }

  private void removeCustomInternal(final String iName) {
    setCustomInternal(iName, null);
  }

  private void setCustomInternal(final String iName, final String iValue) {
    if (iValue == null || "null".equalsIgnoreCase(iValue)) {
      // REMOVE
      for (Iterator<OStorageEntryConfiguration> it = storage.getConfiguration().properties.iterator(); it.hasNext();) {
        final OStorageEntryConfiguration e = it.next();
        if (e.name.equals(iName)) {
          it.remove();
          break;
        }
      }

    } else {
      // SET
      boolean found = false;
      for (OStorageEntryConfiguration e : storage.getConfiguration().properties) {
        if (e.name.equals(iName)) {
          e.value = iValue;
          found = true;
          break;
        }
      }

      if (!found)
        // CREATE A NEW ONE
        storage.getConfiguration().properties.add(new OStorageEntryConfiguration(iName, iValue));
    }

    storage.getConfiguration().update();
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    return storage.getRecordMetadata(rid);
  }

  @Override
  public void freeze() {
    final OFreezableStorage storage = getFreezableStorage();
    if (storage != null) {
      storage.freeze(false);
    }
  }

  private OFreezableStorage getFreezableStorage() {
    OStorage s = getStorage();
    if (s instanceof OFreezableStorage)
      return (OFreezableStorage) s;
    else {
      OLogManager.instance().error(this, "Storage of type " + s.getType() + " does not support freeze operation.");
      return null;
    }
  }

  @Override
  public void release() {
    final OFreezableStorage storage = getFreezableStorage();
    if (storage != null) {
      storage.release();
    }
  }

  @Override
  public void freeze(boolean throwException) {
    final OFreezableStorage storage = getFreezableStorage();
    if (storage != null) {
      storage.freeze(throwException);
    }
  }

  @Override
  public void freezeCluster(int iClusterId) {
    freezeCluster(iClusterId, false);
  }

  @Override
  public void releaseCluster(int iClusterId) {
    final OLocalPaginatedStorage storage;
    if (getStorage() instanceof OLocalPaginatedStorage)
      storage = ((OLocalPaginatedStorage) getStorage());
    else {
      OLogManager.instance().error(this, "We can not freeze non local storage.");
      return;
    }

    storage.release(iClusterId);
  }

  @Override
  public void freezeCluster(int iClusterId, boolean throwException) {
    if (getStorage() instanceof OLocalPaginatedStorage) {
      final OLocalPaginatedStorage paginatedStorage = ((OLocalPaginatedStorage) getStorage());
      paginatedStorage.freeze(throwException, iClusterId);
    } else {
      OLogManager.instance().error(this, "Only local paginated storage supports cluster freeze.");
    }
  }

  public ODatabaseRecord rollback() {
    return rollback(false);
  }

  @Override
  public ODatabaseRecord rollback(boolean force) throws OTransactionException {
    checkOpeness();
    if (currentTx.isActive()) {

      if (!force && currentTx.amountOfNestedTxs() > 1) {
        currentTx.rollback();
        return this;
      }

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onBeforeTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error before tx rollback", t);
        }

      currentTx.rollback(force, -1);

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onAfterTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error after tx rollback", t);
        }
    }

    getLocalCache().clear();

    return this;
  }

  public OTransaction getTransaction() {
    return currentTx;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan) {
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(ORecord iRecord, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORecord iRecord) {
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, null, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORID recordId) {
    return (RET) currentTx.loadRecord(recordId, null, null, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORID iRecordId, final String iFetchPlan) {
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORID iRecordId, String iFetchPlan, final boolean iIgnoreCache,
      final boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET reload(final ORecord iRecord) {
    return reload(iRecord, null, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET reload(final ORecord iRecord, final String iFetchPlan) {
    return reload(iRecord, iFetchPlan, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET reload(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    ORecord record = currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache, false,
        OStorage.LOCKING_STRATEGY.DEFAULT);
    if (record != null && iRecord != record) {
      iRecord.fromStream(record.toStream());
      iRecord.getRecordVersion().copyFrom(record.getRecordVersion());
    } else if (record == null)
      throw new ORecordNotFoundException("Record with rid " + iRecord.getIdentity() + " was not found in database");

    return (RET) record;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET save(final ORecord iContent, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return (RET) save(iContent, (String) null, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET save(final ORecord iContent) {
    return (RET) save(iContent, (String) null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET save(final ORecord iContent, final String iClusterName) {
    return (RET) save(iContent, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET save(final ORecord iContent, final String iClusterName, final OPERATION_MODE iMode,
      boolean iForceCreate, ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return (RET) currentTx.saveRecord(iContent, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseRecord delete(final ORID iRecord) {
    checkOpeness();
    final ORecord rec = iRecord.getRecord();
    if (rec != null)
      rec.delete();
    return this;
  }

  @Override
  public ODatabaseRecord delete(final ORecord iRecord) {
    checkOpeness();
    currentTx.deleteRecord(iRecord, OPERATION_MODE.SYNCHRONOUS);
    return this;
  }

  @Override
  public boolean hide(ORID rid) {
    checkOpeness();
    if (currentTx.isActive())
      throw new ODatabaseException("This operation can be executed only in non tx mode");
    return super.hide(rid);
  }

  @Override
  public ODatabaseRecord delete(final ORecord iRecord, final OPERATION_MODE iMode) {
    currentTx.deleteRecord(iRecord, iMode);
    return this;
  }


  public ORecord getRecordByUserObject(final Object iUserObject, final boolean iCreateIfNotAvailable) {
    return (ORecord) iUserObject;
  }

  public void registerUserObject(final Object iObject, final ORecord iRecord) {
  }

  public void registerUserObjectAfterLinkSave(ORecord iRecord) {
  }

  public Object getUserObjectByRecord(final OIdentifiable record, final String iFetchPlan) {
    return record;
  }

  public boolean existsUserObjectByRID(final ORID iRID) {
    return true;
  }

  public String getType() {
    return TYPE;
  }

  public void setDefaultTransactionMode() {
    if (!(currentTx instanceof OTransactionNoTx))
      currentTx = new OTransactionNoTx(this);
  }

  protected void checkTransaction() {
    if (currentTx == null || currentTx.getStatus() == TXSTATUS.INVALID)
      throw new OTransactionException("Transaction not started");
  }

  private void init() {
    currentTx = new OTransactionNoTx(this);
  }

  @Override
  public <DB extends ODatabase> DB getUnderlying() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OStorage getStorage() {
    return storage;
  }

  @Override
  public void replaceStorage(OStorage iNewStorage) {
    storage = iNewStorage;
  }

  @Override
  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    return storage.callInLock(iCallable, iExclusiveLock);
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, Callable<Object> callable, OCommandOutputListener iListener,
      int compressionLevel, int bufferSize) throws IOException {
    storage.backup(out, options, callable, iListener, compressionLevel, bufferSize);
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable, OCommandOutputListener iListener)
      throws IOException {
    if (storage == null)
      storage = Orient.instance().loadStorage(url);

    getStorage().restore(in, options, callable, iListener);
  }
}
