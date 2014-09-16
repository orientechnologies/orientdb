/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.db.raw;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.Callable;

/**
 * Lower level ODatabase implementation. It's extended or wrapped by all the others.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class ODatabaseRaw extends OListenerManger<ODatabaseListener> implements ODatabase {
  private final Map<String, Object> properties = new HashMap<String, Object>();
  protected String                  url;
  protected OStorage                storage;
  protected STATUS                  status;
  protected OIntent                 currentIntent;
  private ODatabaseRecord           databaseOwner;

  public ODatabaseRaw(final String iURL) {
		super(false);
    if (iURL == null)
      throw new IllegalArgumentException("URL parameter is null");

    try {
      url = iURL.replace('\\', '/');
      status = STATUS.CLOSED;

      // SET DEFAULT PROPERTIES
      setProperty("fetch-max", 50);

    } catch (Throwable t) {
      throw new ODatabaseException("Error on opening database '" + iURL + "'", t);
    }
  }

  public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
    try {
      if (status == STATUS.OPEN)
        throw new IllegalStateException("Database " + getName() + " is already open");

      if (storage == null)
        storage = Orient.instance().loadStorage(url);

      storage.open(iUserName, iUserPassword, properties);

      status = STATUS.OPEN;

    } catch (OStorageException e) {
      // UNREGISTER STORAGE
      Orient.instance().unregisterStorage(storage);

      // PASS THROUGH
      throw e;

    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw new ODatabaseException("Cannot open database", e);
    }
    return (DB) this;
  }

  public <DB extends ODatabase> DB create() {
    return create(null);
  }

  public <DB extends ODatabase> DB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    try {
      if (status == STATUS.OPEN)
        throw new IllegalStateException("Database " + getName() + " is already open");

      if (storage == null)
        storage = Orient.instance().loadStorage(url);

      if (iInitialSettings != null) {
        // SETUP INITIAL SETTINGS
        final OContextConfiguration ctxCfg = storage.getConfiguration().getContextConfiguration();
        for (Map.Entry<OGlobalConfiguration, Object> e : iInitialSettings.entrySet()) {
          ctxCfg.setValue(e.getKey(), e.getValue());
        }
      }

      storage.create(properties);

      status = STATUS.OPEN;
    } catch (Exception e) {
      throw new ODatabaseException("Cannot create database", e);
    }
    return (DB) this;
  }

  public void drop() {
    final Iterable<ODatabaseListener> tmpListeners = getListenersCopy();
    closeOnDelete();

    try {
      if (storage == null)
        storage = Orient.instance().loadStorage(url);

      storage.delete();
      storage = null;

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : tmpListeners)
        try {
          listener.onDelete(this);
        } catch (Throwable t) {
        }

      status = STATUS.CLOSED;
      ODatabaseRecordThreadLocal.INSTANCE.set(null);

    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw new ODatabaseException("Cannot delete database", e);
    }
  }

  @Override
  public OContextConfiguration getConfiguration() {
    if (storage != null)
      return storage.getConfiguration().getContextConfiguration();
    return null;
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, Callable<Object> callable,
      final OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    getStorage().backup(out, options, callable, iListener, compressionLevel, bufferSize);

  }

  /**
   * Executes a restore of a database backup. During the restore the database will be frozen in read-only mode.
   * 
   * @param in
   *          InputStream used to read the backup content. Use a FileInputStream to read a backup on a disk
   * @param options
   *          Backup options as Map<String, Object> object
   * @param callable
   *          Callback to execute when the database is locked
   * @param iListener
   *          Listener called for backup messages
   * @throws IOException
   * @see ODatabaseImport
   */
  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable, final OCommandOutputListener iListener)
      throws IOException {
    if (storage == null)
      storage = Orient.instance().loadStorage(url);

    getStorage().restore(in, options, callable, iListener);
  }

  public void reload() {
    storage.reload();
  }

  public STATUS getStatus() {
    return status;
  }

  public <DB extends ODatabase> DB setStatus(final STATUS status) {
    this.status = status;
    return (DB) this;
  }

  public <DB extends ODatabase> DB setDefaultClusterId(final int iDefClusterId) {
    storage.setDefaultClusterId(iDefClusterId);
    return (DB) this;
  }

  public boolean exists() {
    if (status == STATUS.OPEN)
      return true;

    if (storage == null)
      storage = Orient.instance().loadStorage(url);

    return storage.exists();
  }

  public long countClusterElements(final String iClusterName) {
    final int clusterId = getClusterIdByName(iClusterName);
    if (clusterId < 0)
      throw new IllegalArgumentException("Cluster '" + iClusterName + "' was not found");
    return storage.count(clusterId);
  }

  public long countClusterElements(final int iClusterId) {
    return storage.count(iClusterId);
  }

  public long countClusterElements(final int[] iClusterIds) {
    return storage.count(iClusterIds);
  }

  @Override
  public long countClusterElements(int iCurrentClusterId, boolean countTombstones) {
    return storage.count(iCurrentClusterId, countTombstones);
  }

  @Override
  public long countClusterElements(int[] iClusterIds, boolean countTombstones) {
    return storage.count(iClusterIds, countTombstones);
  }

  public OStorageOperationResult<ORawBuffer> read(final ORecordId iRid, final String iFetchPlan, final boolean iIgnoreCache,
      final boolean loadTombstones, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    if (!iRid.isValid())
      return new OStorageOperationResult<ORawBuffer>(null);

    OFetchHelper.checkFetchPlanValid(iFetchPlan);

    try {
      return storage.readRecord(iRid, iFetchPlan, iIgnoreCache, null, loadTombstones, iLockingStrategy);

    } catch (Throwable t) {
      if (iRid.isTemporary())
        throw new ODatabaseException("Error on retrieving record using temporary RecordId: " + iRid, t);
      else
        throw new ODatabaseException("Error on retrieving record " + iRid + " (cluster: "
            + storage.getPhysicalClusterNameById(iRid.clusterId) + ")", t);
    }
  }

  public OStorageOperationResult<ORecordVersion> save(final ORecordId iRid, boolean updateContent,
																											final byte[] iContent, final ORecordVersion iVersion, final byte iRecordType, final int iMode, boolean iForceCreate,
																											final ORecordCallback<? extends Number> iRecordCreatedCallback, final ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {

    // CHECK IF RECORD TYPE IS SUPPORTED
    Orient.instance().getRecordFactoryManager().getRecordTypeClass(iRecordType);

    try {
      if (iForceCreate || iRid.clusterPosition.isNew()) {
        // CREATE
        final OStorageOperationResult<OPhysicalPosition> ppos = storage.createRecord(iRid, iContent, iVersion, iRecordType, iMode,
            (ORecordCallback<OClusterPosition>) iRecordCreatedCallback);
        return new OStorageOperationResult<ORecordVersion>(ppos.getResult().recordVersion, ppos.isMoved());

      } else {
        // UPDATE
        return storage.updateRecord(iRid, updateContent, iContent, iVersion, iRecordType, iMode, iRecordUpdatedCallback);
      }
    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Throwable t) {
      throw new ODatabaseException("Error on saving record " + iRid, t);
    }
  }

  public OStorageOperationResult<Boolean> delete(final ORecordId rid, final ORecordVersion iVersion, final boolean iRequired,
      final int iMode) {
    try {
      final OStorageOperationResult<Boolean> result = storage.deleteRecord(rid, iVersion, iMode, null);
      if (!result.getResult() && iRequired)
        throw new ORecordNotFoundException("The record with id " + rid + " was not found");
      return result;
    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw new ODatabaseException("Error on deleting record " + rid, e);
    }
  }

  public OStorageOperationResult<Boolean> hide(final ORecordId rid, final int mode) {
    try {
      return storage.hideRecord(rid, mode, null);
    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw new ODatabaseException("Error on deleting record " + rid, e);
    }
  }

  public boolean cleanOutRecord(final ORecordId rid, final ORecordVersion iVersion, final boolean iRequired, final int iMode) {
    try {
      final boolean result = storage.cleanOutRecord(rid, iVersion, iMode, null);
      if (!result && iRequired)
        throw new ORecordNotFoundException("The record with id " + rid + " was not found");
      return result;
    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw new ODatabaseException("Error on deleting record " + rid, e);
    }
  }

  public OStorage getStorage() {
    return storage;
  }

  public void replaceStorage(final OStorage iNewStorage) {
    storage = iNewStorage;
  }

  public boolean isClosed() {
    return status == STATUS.CLOSED || storage.isClosed();
  }

  public String getName() {
    return storage != null ? storage.getName() : url;
  }

  public String getURL() {
    return url != null ? url : storage.getURL();
  }

  public int getClusters() {
    return storage.getClusters();
  }

  public boolean existsCluster(final String iClusterName) {
    return storage.getClusterNames().contains(iClusterName.toLowerCase());
  }

  public int getClusterIdByName(final String iClusterName) {
    return storage.getClusterIdByName(iClusterName.toLowerCase());
  }

  public String getClusterNameById(final int iClusterId) {
    if (iClusterId == -1)
      return null;

    // PHIYSICAL CLUSTER
    return storage.getPhysicalClusterNameById(iClusterId);
  }

  public long getClusterRecordSizeById(final int clusterId) {
    try {
      return storage.getClusterById(clusterId).getRecordsSize();
    } catch (Exception e) {
      throw new ODatabaseException("Error on reading records size for cluster with id '" + clusterId + "'", e);
    }
  }

  public long getClusterRecordSizeByName(final String clusterName) {
    try {
      return storage.getClusterById(getClusterIdByName(clusterName)).getRecordsSize();
    } catch (Exception e) {
      throw new ODatabaseException("Error on reading records size for cluster '" + clusterName + "'", e);
    }
  }

  public int addCluster(String iClusterName, Object... iParameters) {
    return storage.addCluster(iClusterName, false, iParameters);
  }

  public int addCluster(String iClusterName, int iRequestedId, Object... iParameters) {
    return storage.addCluster(iClusterName, iRequestedId, false, iParameters);
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    return storage.dropCluster(iClusterName, iTruncate);
  }

  public boolean dropCluster(int iClusterId, final boolean iTruncate) {
    return storage.dropCluster(iClusterId, iTruncate);
  }

  public Collection<String> getClusterNames() {
    return storage.getClusterNames();
  }

  /**
   * @return always null
   */
  public OLocalRecordCache getLocalCache() {
    return null;
  }

  public int getDefaultClusterId() {
    return storage.getDefaultClusterId();
  }

  public boolean declareIntent(final OIntent iIntent) {
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

  public ODatabaseRecord getDatabaseOwner() {
    return databaseOwner;
  }

  public ODatabaseRaw setOwner(final ODatabaseRecord iOwner) {
    databaseOwner = iOwner;
    return this;
  }

  public Object setProperty(final String iName, final Object iValue) {
    if (iValue == null)
      return properties.remove(iName.toLowerCase());
    else
      return properties.put(iName.toLowerCase(), iValue);
  }

  public Object getProperty(final String iName) {
    return properties.get(iName.toLowerCase());
  }

  public Iterator<Entry<String, Object>> getProperties() {
    return properties.entrySet().iterator();
  }

  public void close() {
    if (status != STATUS.OPEN)
      return;

    if (currentIntent != null) {
      currentIntent.end(this);
      currentIntent = null;
    }

    resetListeners();

    if (storage != null)
      storage.close();

    storage = null;
    status = STATUS.CLOSED;
  }

  public void closeOnDelete() {
    if (status != STATUS.OPEN)
      return;

    if (currentIntent != null) {
      currentIntent.end(this);
      currentIntent = null;
    }

    resetListeners();

    if (storage != null)
      storage.close(true, true);

    storage = null;
    status = STATUS.CLOSED;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(256);
    buffer.append("OrientDB[");
    buffer.append(url != null ? url : "?");
    buffer.append(']');
    if (getStorage() != null) {
      buffer.append(" (users: ");
      buffer.append(getStorage().getUsers());
      buffer.append(')');
    }
    return buffer.toString();
  }

  public Object get(final ATTRIBUTES iAttribute) {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    switch (iAttribute) {
    case STATUS:
      return getStatus();
    case DEFAULTCLUSTERID:
      return getDefaultClusterId();
    case TYPE:
      final ODatabaseRecord db = getDatabaseOwner();

      return db.getMetadata().getSchema().existsClass("V") ? "graph" : "document";
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

  public <DB extends ODatabase> DB set(final ATTRIBUTES iAttribute, final Object iValue) {
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
      storage.getConfiguration().dateFormat = stringValue;
      storage.getConfiguration().update();
      break;

    case DATETIMEFORMAT:
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
      int indx = stringValue!=null?stringValue.indexOf('='):-1;
      if (indx<0) {
        if ("clear".equalsIgnoreCase(stringValue)) {
          clearCustomInternal();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        String customName = stringValue.substring(0, indx).trim();
        String customValue = stringValue.substring(indx+1).trim();
        if(customValue.isEmpty()) removeCustomInternal(customName);
        else setCustomInternal(customName, customValue);
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

  public String getCustom(final String iName) {
    for (OStorageEntryConfiguration e : storage.getConfiguration().properties) {
      if (e.name.equals(iName))
        return e.value;
    }
    return null;
  }
  
  public void removeCustomInternal(final String iName) {
	  setCustomInternal(iName, null);
  }

  public void setCustomInternal(final String iName, final String iValue) {
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

  public void clearCustomInternal() {
    storage.getConfiguration().properties.clear();
  }

  public <V> V callInLock(final Callable<V> iCallable, final boolean iExclusiveLock) {
    return storage.callInLock(iCallable, iExclusiveLock);
  }

  @Override
  public <V> V callInRecordLock(final Callable<V> iCallable, final ORID rid, final boolean iExclusiveLock) {
    return storage.callInRecordLock(iCallable, rid, iExclusiveLock);
  }

  @Override
  public ORecordMetadata getRecordMetadata(final ORID rid) {
    return storage.getRecordMetadata(rid);
  }

  public void callOnOpenListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext();)
      it.next().onOpen(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : getListenersCopy())
      try {
        listener.onOpen(getDatabaseOwner());
      } catch (Throwable t) {
        OLogManager.instance().error(this, "Error on invoking onOpen() against listener: %s", t, listener);
      }
  }

  public void callOnCloseListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext();)
      it.next().onClose(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : getListenersCopy())
      try {
        listener.onClose(getDatabaseOwner());
      } catch (Throwable t) {
        OLogManager.instance().error(this, "Error on invoking onClose() against listener: %s", t, listener);
      }
  }

  public long getSize() {
    return storage.getSize();
  }

  public void freeze() {
    final OFreezableStorage storage = getFreezableStorage();
    if (storage != null) {
      storage.freeze(false);
    }
  }

  public void freeze(final boolean throwException) {
    final OFreezableStorage storage = getFreezableStorage();
    if (storage != null) {
      storage.freeze(throwException);
    }
  }

  public void release() {
    final OFreezableStorage storage = getFreezableStorage();
    if (storage != null) {
      storage.release();
    }
  }

  @Override
  public void freezeCluster(final int iClusterId) {
    freezeCluster(iClusterId, false);
  }

  @Override
  public void releaseCluster(final int iClusterId) {
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
  public void freezeCluster(final int iClusterId, final boolean throwException) {
    if (getStorage() instanceof OLocalPaginatedStorage) {
      final OLocalPaginatedStorage paginatedStorage = ((OLocalPaginatedStorage) getStorage());
      paginatedStorage.freeze(throwException, iClusterId);
    } else {
      OLogManager.instance().error(this, "Only local paginated storage supports cluster freeze.");
    }
  }

  protected boolean isClusterBoundedToClass(final int iClusterId) {
    return false;
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
}
