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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

@SuppressWarnings("unchecked")
public abstract class ODatabaseWrapperAbstract<DB extends ODatabaseInternal, T> implements ODatabaseInternal<T> {
  protected DB                   underlying;
  protected ODatabaseInternal<?> databaseOwner;

  public ODatabaseWrapperAbstract(final DB iDatabase) {
    underlying = iDatabase;
    databaseOwner = this;
    Orient.instance().getDatabaseFactory().register(databaseOwner);
  }

  public <THISDB extends ODatabase> THISDB open(final String iUserName, final String iUserPassword) {
    underlying.open(iUserName, iUserPassword);
    return (THISDB) this;
  }

  public <THISDB extends ODatabase> THISDB open(final OToken iToken) {
    underlying.open(iToken);
    Orient.instance().getDatabaseFactory().register(databaseOwner);
    return (THISDB) this;
  }

  @Override
  public ODatabase activateOnCurrentThread() {
    return underlying.activateOnCurrentThread();
  }

  @Override
  public boolean isActiveOnCurrentThread() {
    return underlying.isActiveOnCurrentThread();
  }

  public <THISDB extends ODatabase> THISDB create() {
    return create(null);
  }

  public <THISDB extends ODatabase> THISDB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    underlying.create(iInitialSettings);
    return (THISDB) this;
  }

  public boolean exists() {
    return underlying.exists();
  }

  public void reload() {
    underlying.reload();
  }

  @Override
  public OContextConfiguration getConfiguration() {
    return underlying.getConfiguration();
  }

  /**
   * Executes a backup of the database. During the backup the database will be frozen in read-only mode.
   * 
   * @param out
   *          OutputStream used to write the backup content. Use a FileOutputStream to make the backup persistent on disk
   * @param options
   *          Backup options as Map<String, Object> object
   * @param callable
   *          Callback to execute when the database is locked
   * @param iListener
   *          Listener called for backup messages
   * @param compressionLevel
   *          ZIP Compression level between 0 (no compression) and 9 (maximum). The bigger is the compression, the smaller will be
   *          the final backup content, but will consume more CPU and time to execute
   * @param bufferSize
   *          Buffer size in bytes, the bigger is the buffer, the more efficient will be the compression
   * @throws IOException
   */
  @Override
  public List<String> backup(OutputStream out, Map<String, Object> options, Callable<Object> callable,
      final OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    return underlying.backup(out, options, callable, iListener, compressionLevel, bufferSize);
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
    underlying.restore(in, options, callable, iListener);
  }

  public void close() {
    underlying.close();
  }

  public void replaceStorage(OStorage iNewStorage) {
    underlying.replaceStorage(iNewStorage);
  }

  public void drop() {
    underlying.drop();
    Orient.instance().getDatabaseFactory().unregister(databaseOwner);
  }

  public STATUS getStatus() {
    return underlying.getStatus();
  }

  public <THISDB extends ODatabase> THISDB setStatus(final STATUS iStatus) {
    underlying.setStatus(iStatus);
    return (THISDB) this;
  }

  public String getName() {
    return underlying.getName();
  }

  public String getURL() {
    return underlying.getURL();
  }

  public OStorage getStorage() {
    return underlying.getStorage();
  }

  public OLocalRecordCache getLocalCache() {
    return underlying.getLocalCache();
  }

  public boolean isClosed() {
    return underlying.isClosed();
  }

  public long countClusterElements(final int iClusterId) {
    checkOpeness();
    return underlying.countClusterElements(iClusterId);
  }

  public long countClusterElements(final int[] iClusterIds) {
    checkOpeness();
    return underlying.countClusterElements(iClusterIds);
  }

  public long countClusterElements(final String iClusterName) {
    checkOpeness();
    return underlying.countClusterElements(iClusterName);
  }

  @Override
  public long countClusterElements(int iClusterId, boolean countTombstones) {
    checkOpeness();
    return underlying.countClusterElements(iClusterId, countTombstones);
  }

  @Override
  public long countClusterElements(int[] iClusterIds, boolean countTombstones) {
    checkOpeness();
    return underlying.countClusterElements(iClusterIds, countTombstones);
  }

  public int getClusters() {
    checkOpeness();
    return underlying.getClusters();
  }

  public boolean existsCluster(String iClusterName) {
    checkOpeness();
    return underlying.existsCluster(iClusterName);
  }

  public Collection<String> getClusterNames() {
    checkOpeness();
    return underlying.getClusterNames();
  }

  public int getClusterIdByName(final String iClusterName) {
    checkOpeness();
    return underlying.getClusterIdByName(iClusterName);
  }

  public String getClusterNameById(final int iClusterId) {
    checkOpeness();
    return underlying.getClusterNameById(iClusterId);
  }

  public long getClusterRecordSizeById(int iClusterId) {
    return underlying.getClusterRecordSizeById(iClusterId);
  }

  public long getClusterRecordSizeByName(String iClusterName) {
    return underlying.getClusterRecordSizeByName(iClusterName);
  }

  public int addCluster(String iClusterName, int iRequestedId, Object... iParameters) {
    checkOpeness();
    return underlying.addCluster(iClusterName, iRequestedId, iParameters);
  }

  public int addCluster(final String iClusterName, final Object... iParameters) {
    checkOpeness();
    return underlying.addCluster(iClusterName, iParameters);
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    getLocalCache().freeCluster(getClusterIdByName(iClusterName));
    return underlying.dropCluster(iClusterName, true);
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    getLocalCache().freeCluster(iClusterId);
    return underlying.dropCluster(iClusterId, true);
  }

  public int getDefaultClusterId() {
    checkOpeness();
    return underlying.getDefaultClusterId();
  }

  public boolean declareIntent(final OIntent iIntent) {
    return underlying.declareIntent(iIntent);
  }

  public <DBTYPE extends ODatabase> DBTYPE getUnderlying() {
    return (DBTYPE) underlying;
  }

  public ODatabaseInternal<?> getDatabaseOwner() {
    return databaseOwner;
  }

  public ODatabaseInternal<?> setDatabaseOwner(final ODatabaseInternal<?> iOwner) {
    databaseOwner = iOwner;
    return this;
  }

  @Override
  public boolean equals(final Object iOther) {
    if (!(iOther instanceof ODatabase))
      return false;

    final ODatabase other = (ODatabase) iOther;

    return other.getName().equals(getName());
  }

  @Override
  public String toString() {
    return underlying.toString();
  }

  public Object setProperty(final String iName, final Object iValue) {
    return underlying.setProperty(iName, iValue);
  }

  public Object getProperty(final String iName) {
    return underlying.getProperty(iName);
  }

  public Iterator<Entry<String, Object>> getProperties() {
    return underlying.getProperties();
  }

  public Object get(final ATTRIBUTES iAttribute) {
    return underlying.get(iAttribute);
  }

  public <THISDB extends ODatabase> THISDB set(final ATTRIBUTES attribute, final Object iValue) {
    return (THISDB) underlying.set(attribute, iValue);
  }

  public void registerListener(final ODatabaseListener iListener) {
    underlying.registerListener(iListener);
  }

  public void unregisterListener(final ODatabaseListener iListener) {
    underlying.unregisterListener(iListener);
  }

  @Override
  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    return getStorage().callInLock(iCallable, iExclusiveLock);
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    return underlying.getRecordMetadata(rid);
  }

  public long getSize() {
    return underlying.getSize();
  }

  public void freeze(boolean throwException) {
    underlying.freeze(throwException);
  }

  public void freeze() {
    underlying.freeze();
  }

  public void release() {
    underlying.release();
  }

  @Override
  public void freezeCluster(int iClusterId, boolean throwException) {
    underlying.freezeCluster(iClusterId, throwException);
  }

  @Override
  public void freezeCluster(int iClusterId) {
    underlying.freezeCluster(iClusterId);
  }

  @Override
  public void releaseCluster(int iClusterId) {
    underlying.releaseCluster(iClusterId);
  }

  protected void checkOpeness() {
    if (isClosed())
      throw new ODatabaseException("Database '" + getURL() + "' is closed");
  }
}
