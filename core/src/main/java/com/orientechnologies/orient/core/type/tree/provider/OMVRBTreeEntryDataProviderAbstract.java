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
package com.orientechnologies.orient.core.type.tree.provider;

import java.lang.ref.WeakReference;

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordListener;
import com.orientechnologies.orient.core.record.impl.ORecordBytesLazy;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.OVersionFactory;

public abstract class OMVRBTreeEntryDataProviderAbstract<K, V> implements OMVRBTreeEntryDataProvider<K, V>, OSerializableStream,
    ORecordListener {
  private static final long                         serialVersionUID = 1L;

  protected final OMVRBTreeProviderAbstract<K, V>   treeDataProvider;

  protected int                                     size             = 0;
  protected int                                     pageSize;

  protected ORecordId                               parentRid;
  protected ORecordId                               leftRid;
  protected ORecordId                               rightRid;
  protected boolean                                 color            = OMVRBTree.RED;
  protected ORecordBytesLazy                        record;
  protected OMemoryStream                           stream;
  protected WeakReference<OIdentityChangedListener> identityChangedListener;

  public OMVRBTreeEntryDataProviderAbstract(final OMVRBTreeProviderAbstract<K, V> iTreeDataProvider, final int iFixedSize) {
    this(iTreeDataProvider, null);
    pageSize = treeDataProvider.getDefaultPageSize();
    stream = new OMemoryStream(iFixedSize);
  }

  public OMVRBTreeEntryDataProviderAbstract(final OMVRBTreeProviderAbstract<K, V> iTreeDataProvider, final ORID iRID) {
    treeDataProvider = iTreeDataProvider;

    parentRid = new ORecordId();
    leftRid = new ORecordId();
    rightRid = new ORecordId();

    record = (ORecordBytesLazy) new ORecordBytesLazy(this).unpin();
    if (iRID != null) {
      record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
      if (treeDataProvider.storage == null)
        load(OMVRBTreeProviderAbstract.getDatabase());
      else
        load(treeDataProvider.storage);
    } else
      record.setIdentity(new ORecordId());

    if (record.getIdentity().isNew() || record.getIdentity().isTemporary())
      record.addListener(this);
  }

  protected void load(final ODatabaseRecord iDb) {
    try {
      record.reload();
    } catch (Exception e) {
      // ERROR, MAYBE THE RECORD WASN'T CREATED
      OLogManager.instance().warn(this, "Error on loading index node record %s", e, record.getIdentity());
    }
    record.recycle(this);
    fromStream(record.toStream());
  }

  protected void load(final OStorage iStorage) {
    final ORawBuffer raw = iStorage.readRecord((ORecordId) record.getIdentity(), null, false, null, false).getResult();
    record.fill((ORecordId) record.getIdentity(), raw.version, raw.buffer, false);
    fromStream(raw.buffer);
  }

  public ORID getIdentity() {
    return record.getIdentity();
  }

  public int getSize() {
    return size;
  }

  public int getPageSize() {
    return pageSize;
  }

  public ORID getParent() {
    return parentRid;
  }

  public ORID getLeft() {
    return leftRid;
  }

  public ORID getRight() {
    return rightRid;
  }

  public boolean setLeft(final ORID iRid) {
    if (leftRid.equals(iRid))
      return false;
    leftRid.copyFrom(iRid);
    return setDirty();
  }

  public boolean setRight(final ORID iRid) {
    if (rightRid.equals(iRid))
      return false;
    rightRid.copyFrom(iRid);
    return setDirty();
  }

  public boolean setParent(final ORID iRid) {
    if (parentRid.equals(iRid))
      return false;
    parentRid.copyFrom(iRid);
    return setDirty();
  }

  public boolean getColor() {
    return color;
  }

  public boolean setColor(final boolean iColor) {
    this.color = iColor;
    return setDirty();
  }

  public boolean isEntryDirty() {
    return record.isDirty();
  }

  public void save() {
    if (treeDataProvider.storage == null)
      save(OMVRBTreeProviderAbstract.getDatabase());
    else
      save(treeDataProvider.storage);
  }

  protected void save(final ODatabaseRecord iDb) {
    if (iDb == null) {
      throw new IllegalStateException(
          "Current thread has no database set and the tree cannot be saved correctly. Ensure that the database is closed before the application if off.");
    }
    record.save(treeDataProvider.clusterName);

    if (!record.getIdentity().isTemporary())
      record.removeListener(this);
  }

  protected void save(final OStorage iStorage) {
    record.fromStream(toStream());
    if (record.getIdentity().isValid())
      // UPDATE IT WITHOUT VERSION CHECK SINCE ALL IT'S LOCKED
      record.getRecordVersion().copyFrom(
          iStorage.updateRecord((ORecordId) record.getIdentity(), record.toStream(),
              OVersionFactory.instance().createUntrackedVersion(), record.getRecordType(), (byte) 0, null).getResult());
    else {
      // CREATE IT
      if (record.getIdentity().getClusterId() == ORID.CLUSTER_ID_INVALID)
        ((ORecordId) record.getIdentity()).clusterId = treeDataProvider.clusterId;

      final OPhysicalPosition ppos = iStorage.createRecord(0, (ORecordId) record.getIdentity(), record.toStream(),
          OVersionFactory.instance().createVersion(), record.getRecordType(), (byte) 0, null).getResult();

      record.setIdentity(record.getIdentity().getClusterId(), ppos.clusterPosition);
      record.getRecordVersion().copyFrom(ppos.recordVersion);
    }
    record.unsetDirty();

    if (!record.getIdentity().isTemporary())
      record.removeListener(this);
  }

  public void delete() {
    record.removeListener(this);
    if (treeDataProvider.storage == null)
      delete((ODatabaseRecord) null);
    else
      delete(treeDataProvider.storage);
  }

  protected void delete(final ODatabaseRecord iDb) {
    record.removeListener(this);
    record.delete();
  }

  protected void delete(final OStorage iSt) {
    record.removeListener(this);
    iSt.deleteRecord((ORecordId) record.getIdentity(), record.getRecordVersion(), (byte) 0, null);
  }

  public void onEvent(ORecord<?> record, ORecordListener.EVENT event) {
    if (ORecordListener.EVENT.IDENTITY_CHANGED.equals(event))
      if (identityChangedListener != null) {
        final OIdentityChangedListener listener = identityChangedListener.get();
        if (listener != null)
          listener.onIdentityChanged(record.getIdentity());
      }
  }

  public void setIdentityChangedListener(final OIdentityChangedListener listener) {
    this.identityChangedListener = new WeakReference<OIdentityChangedListener>(listener);
  }

  public void removeIdentityChangedListener(final OIdentityChangedListener listener) {
    if (identityChangedListener != null) {
      final OIdentityChangedListener identityListener = identityChangedListener.get();
      if (identityListener != null && identityListener.equals(listener))
        identityChangedListener = null;
    }
  }

  public void clear() {
    if (stream != null) {
      stream.close();
      stream = null;
    }
    record.removeListener(this);
    record.recycle(null);
    record = null;
    size = 0;
  }

  protected boolean setDirty() {
    if (record.isDirty())
      return false;
    record.setDirty();
    return true;
  }

  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("mvrb-tree entry ");
    buffer.append(record.getIdentity());
    buffer.append(" (size=");
    buffer.append(size);
    if (size > 0) {
      buffer.append(" [");
      if (size > 1) {
        buffer.append(getKeyAt(0));
        buffer.append(" ... ");
        buffer.append(getKeyAt(size - 1));
      } else {
        buffer.append(getKeyAt(0));
      }
      buffer.append("]");
    }
    buffer.append(")");
    return buffer.toString();
  }
}
