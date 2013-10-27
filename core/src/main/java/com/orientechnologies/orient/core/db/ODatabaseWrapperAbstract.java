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
package com.orientechnologies.orient.core.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLevel1RecordCache;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;

@SuppressWarnings("unchecked")
public abstract class ODatabaseWrapperAbstract<DB extends ODatabase> implements ODatabase {
  protected DB                  underlying;
  protected ODatabaseComplex<?> databaseOwner;

  public ODatabaseWrapperAbstract(final DB iDatabase) {
    underlying = iDatabase;
    databaseOwner = (ODatabaseComplex<?>) this;
  }

  @Override
  public void finalize() {
    // close();
  }

  public <THISDB extends ODatabase> THISDB open(final String iUserName, final String iUserPassword) {
    underlying.open(iUserName, iUserPassword);
    Orient.instance().getDatabaseFactory().register(databaseOwner);
    return (THISDB) this;
  }

  public <THISDB extends ODatabase> THISDB create() {
    underlying.create();
    Orient.instance().getDatabaseFactory().register(databaseOwner);
    return (THISDB) this;
  }

  public boolean exists() {
    return underlying.exists();
  }

  public void reload() {
    underlying.reload();
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, Callable<Object> callable) throws IOException {
    underlying.backup(out, options, callable);
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable) throws IOException {
    underlying.restore(in, options, callable);
  }

  public void close() {
    underlying.close();
    Orient.instance().getDatabaseFactory().unregister(databaseOwner);
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

  public OLevel1RecordCache getLevel1Cache() {
    return underlying.getLevel1Cache();
  }

  public OLevel2RecordCache getLevel2Cache() {
    return getStorage().getLevel2Cache();
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

  public String getClusterType(final String iClusterName) {
    checkOpeness();
    return underlying.getClusterType(iClusterName);
  }

  public int getDataSegmentIdByName(final String iDataSegmentName) {
    checkOpeness();
    return underlying.getDataSegmentIdByName(iDataSegmentName);
  }

  public String getDataSegmentNameById(final int iDataSegmentId) {
    checkOpeness();
    return underlying.getDataSegmentNameById(iDataSegmentId);
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

  public int addCluster(final String iType, final String iClusterName, final String iLocation, final String iDataSegmentName,
      final Object... iParameters) {
    checkOpeness();
    return underlying.addCluster(iType, iClusterName, iLocation, iDataSegmentName, iParameters);
  }

  public int addCluster(String iType, String iClusterName, int iRequestedId, String iLocation, String iDataSegmentName,
      Object... iParameters) {
    return underlying.addCluster(iType, iClusterName, iRequestedId, iLocation, iDataSegmentName, iParameters);
  }

  public int addCluster(final String iClusterName, final CLUSTER_TYPE iType, final Object... iParameters) {
    checkOpeness();
    return underlying.addCluster(iType.toString(), iClusterName, null, null, iParameters);
  }

  public int addCluster(String iClusterName, CLUSTER_TYPE iType) {
    checkOpeness();
    return underlying.addCluster(iType.toString(), iClusterName, null, null);
  }

  public boolean dropDataSegment(final String name) {
    return underlying.dropDataSegment(name);
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    getLevel1Cache().freeCluster(getClusterIdByName(iClusterName));
    return underlying.dropCluster(iClusterName, true);
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    getLevel1Cache().freeCluster(iClusterId);
    return underlying.dropCluster(iClusterId, true);
  }

  public int addDataSegment(final String iSegmentName, final String iLocation) {
    checkOpeness();
    return underlying.addDataSegment(iSegmentName, iLocation);
  }

  public int getDefaultClusterId() {
    checkOpeness();
    return underlying.getDefaultClusterId();
  }

  public boolean declareIntent(final OIntent iIntent) {
    checkOpeness();
    return underlying.declareIntent(iIntent);
  }

  public <DBTYPE extends ODatabase> DBTYPE getUnderlying() {
    return (DBTYPE) underlying;
  }

  public ODatabaseComplex<?> getDatabaseOwner() {
    return databaseOwner;
  }

  public ODatabaseComplex<?> setDatabaseOwner(final ODatabaseComplex<?> iOwner) {
    databaseOwner = iOwner;
    return (ODatabaseComplex<?>) this;
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

  public <V> V callInLock(final Callable<V> iCallable, final boolean iExclusiveLock) {
    return getStorage().callInLock(iCallable, iExclusiveLock);
  }

  @Override
  public <V> V callInRecordLock(Callable<V> iCallable, ORID rid, boolean iExclusiveLock) {
    return underlying.callInRecordLock(iCallable, rid, iExclusiveLock);
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    return underlying.getRecordMetadata(rid);
  }

  public long getSize() {
    return underlying.getSize();
  }

  protected void checkOpeness() {
    if (isClosed())
      throw new ODatabaseException("Database '" + getURL() + "' is closed");
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
}
