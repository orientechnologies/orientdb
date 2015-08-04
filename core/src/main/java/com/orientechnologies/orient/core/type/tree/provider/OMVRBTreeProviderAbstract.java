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
package com.orientechnologies.orient.core.type.tree.provider;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.OVersionFactory;

public abstract class OMVRBTreeProviderAbstract<K, V> implements OMVRBTreeProvider<K, V>, OSerializableStream {
  private static final long serialVersionUID = 1L;

  protected final String    clusterName;
  protected final int       clusterId;
  protected final ORecord   record;
  protected final OStorage  storage;
  protected int             size;
  protected int             pageSize;
  protected ORecordId       root;
  protected int             keySize          = 1;

  public OMVRBTreeProviderAbstract(final ORecord iRecord, final OStorage iStorage, final String iClusterName) {
    storage = iStorage;
    clusterName = iClusterName;
    if (storage != null) {
      if (clusterName != null)
        clusterId = storage.getClusterIdByName(iClusterName);
      else
        clusterId = storage.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);
    } else {
      // CLUSTER ID NOT USED FOR DATABASE INDEX
      clusterId = -1;
    }

    record = iRecord;
    ORecordInternal.setIdentity(record, new ORecordId());
    updateConfig();
  }

  public int getKeySize() {
    return keySize;
  }

  public void setKeySize(int keySize) {
    this.keySize = keySize;
  }

  public int getSize() {
    return size;
  }

  public int getDefaultPageSize() {
    return pageSize;
  }

  public int getClusterId() {
    return clusterId;
  }

  public ORID getRoot() {
    return root;
  }

  public boolean setSize(final int iSize) {
    if (iSize != size) {
      size = iSize;
      return setDirty();
    }
    return false;
  }

  public boolean setRoot(final ORID iRid) {
    if (root == null)
      root = new ORecordId();

    if (iRid == null)
      root.reset();
    else if (!iRid.equals(root))
      root.copyFrom(iRid);

    return setDirty();
  }

  public boolean isDirty() {
    return record.isDirty();
  }

  /**
   * Set the tree as dirty. This happens on change of root.
   * 
   * @return
   */
  public boolean setDirty() {
    if (record.isDirty())
      return false;
    record.setDirty();
    return true;
  }

  public boolean updateConfig() {
    boolean isChanged = false;

    int newSize = OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.getValueAsInteger();
    if (newSize != pageSize) {
      pageSize = newSize;
      isChanged = true;
    }
    return isChanged ? setDirty() : false;
  }

  public void load() {
    if (storage == null)
      load(getDatabase());
    else
      load(storage);
  }

  protected void load(final ODatabaseDocument iDb) {
    if (!record.getIdentity().isValid())
      return;
    record.reload();
    fromStream(record.toStream());
  }

  protected void load(final OStorage iSt) {
    if (!record.getIdentity().isValid())
      // NOTHING TO LOAD
      return;
    ORawBuffer raw = iSt.readRecord((ORecordId) record.getIdentity(), null, false, null).getResult();
    if (raw == null)
      throw new OConfigurationException("Cannot load map with id " + record.getIdentity());
    record.getRecordVersion().copyFrom(raw.version);
    fromStream(raw.buffer);
  }

  protected void save(final ODatabaseDocument iDb) {
    for (int i = 0; i < 3; ++i)
      try {
        record.fromStream(toStream());
        record.setDirty();
        record.save(clusterName);
        break;
      } catch (OConcurrentModificationException e) {
        record.reload();
      }
  }

  public void save() {
    if (storage == null)
      save(getDatabase());
    else
      save(storage);
  }

  protected void save(final OStorage iSt) {
    record.fromStream(toStream());
    if (record.getIdentity().isValid())
      // UPDATE IT WITHOUT VERSION CHECK SINCE ALL IT'S LOCKED
      record.getRecordVersion().copyFrom(
          iSt.updateRecord((ORecordId) record.getIdentity(), true, record.toStream(),
              OVersionFactory.instance().createUntrackedVersion(), ORecordInternal.getRecordType(record), (byte) 0, null)
              .getResult());
    else {
      // CREATE IT
      if (record.getIdentity().getClusterId() == ORID.CLUSTER_ID_INVALID)
        ((ORecordId) record.getIdentity()).clusterId = clusterId;

      final OPhysicalPosition ppos = iSt.createRecord((ORecordId) record.getIdentity(), record.toStream(),
          OVersionFactory.instance().createVersion(), ORecordInternal.getRecordType(record), (byte) 0, null).getResult();
      record.getRecordVersion().copyFrom(ppos.recordVersion);

    }
    ORecordInternal.unsetDirty(record);
  }

  public void delete() {
    if (storage == null)
      delete(getDatabase());
    else
      delete(storage);
    root = null;
  }

  protected void delete(final ODatabaseDocument iDb) {
    for (int i = 0; i < 3; ++i)
      try {
        iDb.delete(record);
        break;
      } catch (OConcurrentModificationException e) {
        record.reload();
      }
  }

  protected void delete(final OStorage iSt) {
    iSt.deleteRecord((ORecordId) record.getIdentity(), record.getRecordVersion(), (byte) 0, null);
  }

  public String toString() {
    return "index " + record.getIdentity();
  }

  @Override
  public int hashCode() {
    final ORID rid = record.getIdentity();
    return rid == null ? 0 : rid.hashCode();
  }

  public ORecord getRecord() {
    return record;
  }

  protected static ODatabaseDocument getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public String getClusterName() {
    return clusterName;
  }
}
