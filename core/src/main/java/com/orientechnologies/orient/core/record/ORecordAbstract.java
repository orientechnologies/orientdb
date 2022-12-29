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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cluster.OOfflineClusterException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

@SuppressWarnings({"unchecked", "serial"})
public abstract class ORecordAbstract implements ORecord {
  public static final String BASE_FORMAT =
      "rid,version,class,type,attribSameRow,keepTypes,alwaysFetchEmbedded";
  public static final String DEFAULT_FORMAT = BASE_FORMAT + "," + "fetchPlan:*:0";
  public static final String OLD_FORMAT_WITH_LATE_TYPES = BASE_FORMAT + "," + "fetchPlan:*:0";
  // TODO: take new format
  // public static final String DEFAULT_FORMAT = OLD_FORMAT_WITH_LATE_TYPES;
  protected ORecordId recordId;
  protected int recordVersion = 0;

  protected byte[] source;
  protected int size;

  protected transient ORecordSerializer recordFormat;
  protected boolean dirty = true;
  protected boolean contentChanged = true;
  protected ORecordElement.STATUS status = ORecordElement.STATUS.LOADED;

  private transient Set<OIdentityChangeListener> newIdentityChangeListeners = null;
  protected ODirtyManager dirtyManager;

  public ORecordAbstract() {}

  public ORecordAbstract(final byte[] iSource) {
    source = iSource;
    size = iSource.length;
    unsetDirty();
  }

  public ORID getIdentity() {
    return recordId;
  }

  protected ORecordAbstract setIdentity(final ORecordId iIdentity) {
    recordId = iIdentity;
    return this;
  }

  @Override
  public ORecordElement getOwner() {
    return null;
  }

  public ORecord getRecord() {
    return this;
  }

  public boolean detach() {
    return true;
  }

  public ORecordAbstract clear() {
    setDirty();
    return this;
  }

  public ORecordAbstract reset() {
    status = ORecordElement.STATUS.LOADED;
    recordVersion = 0;
    size = 0;

    source = null;
    setDirty();
    if (recordId != null) recordId.reset();

    return this;
  }

  public byte[] toStream() {
    if (source == null) source = recordFormat.toStream(this);

    return source;
  }

  public ORecordAbstract fromStream(final byte[] iRecordBuffer) {
    dirty = false;
    contentChanged = false;
    dirtyManager = null;
    source = iRecordBuffer;
    size = iRecordBuffer != null ? iRecordBuffer.length : 0;
    status = ORecordElement.STATUS.LOADED;

    return this;
  }

  protected ORecordAbstract fromStream(final byte[] iRecordBuffer, ODatabaseDocumentInternal db) {
    dirty = false;
    contentChanged = false;
    dirtyManager = null;
    source = iRecordBuffer;
    size = iRecordBuffer != null ? iRecordBuffer.length : 0;
    status = ORecordElement.STATUS.LOADED;

    return this;
  }

  public ORecordAbstract setDirty() {
    if (!dirty && status != STATUS.UNMARSHALLING) {
      dirty = true;
      source = null;
    }

    contentChanged = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (!dirty && status != STATUS.UNMARSHALLING) {
      dirty = true;
      source = null;
    }
  }

  public boolean isDirty() {
    return dirty;
  }

  public <RET extends ORecord> RET fromJSON(final String iSource, final String iOptions) {
    // ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null, iOptions);
    ORecordSerializerJSON.INSTANCE.fromString(
        iSource, this, null, iOptions, false); // Add new parameter to accommodate new API,
    // nothing change
    return (RET) this;
  }

  public <RET extends ORecord> RET fromJSON(final String iSource) {
    ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null);
    return (RET) this;
  }

  // Add New API to load record if rid exist
  public <RET extends ORecord> RET fromJSON(final String iSource, boolean needReload) {
    return (RET) ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null, needReload);
  }

  public <RET extends ORecord> RET fromJSON(final InputStream iContentResult) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    OIOUtils.copyStream(iContentResult, out);
    ORecordSerializerJSON.INSTANCE.fromString(out.toString(), this, null);
    return (RET) this;
  }

  public String toJSON() {
    return toJSON(DEFAULT_FORMAT);
  }

  public String toJSON(final String format) {
    return ORecordSerializerJSON.INSTANCE
        .toString(this, new StringBuilder(1024), format == null ? "" : format)
        .toString();
  }

  public void toJSON(final String format, final OutputStream stream) throws IOException {
    stream.write(toJSON(format).getBytes());
  }

  public void toJSON(final OutputStream stream) throws IOException {
    stream.write(toJSON().getBytes());
  }

  @Override
  public String toString() {
    return (recordId.isValid() ? recordId : "")
        + (source != null ? Arrays.toString(source) : "[]")
        + " v"
        + recordVersion;
  }

  public int getVersion() {
    // checkForLoading();
    return recordVersion;
  }

  protected void setVersion(final int iVersion) {
    recordVersion = iVersion;
  }

  public ORecordAbstract unload() {
    status = ORecordElement.STATUS.NOT_LOADED;
    source = null;
    unsetDirty();
    return this;
  }

  public ORecord load() {
    if (!getIdentity().isValid())
      throw new ORecordNotFoundException(
          getIdentity(), "The record has no id, probably it's new or transient yet ");

    final ORecord result = getDatabase().load(this);

    if (result == null) throw new ORecordNotFoundException(getIdentity());

    return result;
  }

  public ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  public ODatabaseDocumentInternal getDatabaseIfDefined() {
    return ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  public ORecord reload() {
    return reload(null, true, true);
  }

  public ORecord reload(final String fetchPlan) {
    return reload(fetchPlan, true);
  }

  public ORecord reload(final String fetchPlan, final boolean ignoreCache) {
    return reload(fetchPlan, ignoreCache, true);
  }

  @Override
  public ORecord reload(String fetchPlan, boolean ignoreCache, boolean force)
      throws ORecordNotFoundException {
    if (!getIdentity().isValid())
      throw new ORecordNotFoundException(
          getIdentity(), "The record has no id. It is probably new or still transient");

    try {
      getDatabase().reload(this, fetchPlan, ignoreCache, force);

      return this;

    } catch (OOfflineClusterException e) {
      throw e;
    } catch (ORecordNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(new ORecordNotFoundException(getIdentity()), e);
    }
  }

  public ORecordAbstract save() {
    return save(false);
  }

  public ORecordAbstract save(final String iClusterName) {
    return save(iClusterName, false);
  }

  public ORecordAbstract save(boolean forceCreate) {
    getDatabase().save(this, ODatabase.OPERATION_MODE.SYNCHRONOUS, forceCreate, null, null);
    return this;
  }

  public ORecordAbstract save(String iClusterName, boolean forceCreate) {
    return getDatabase()
        .save(this, iClusterName, ODatabase.OPERATION_MODE.SYNCHRONOUS, forceCreate, null, null);
  }

  public ORecordAbstract delete() {
    getDatabase().delete(this);
    return this;
  }

  public int getSize() {
    return size;
  }

  @Override
  public void lock(final boolean iExclusive) {
    ODatabaseRecordThreadLocal.instance()
        .get()
        .getTransaction()
        .lockRecord(
            this,
            iExclusive
                ? OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK
                : OStorage.LOCKING_STRATEGY.SHARED_LOCK);
  }

  @Override
  public boolean isLocked() {
    return ODatabaseRecordThreadLocal.instance().get().getTransaction().isLockedRecord(this);
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    return ODatabaseRecordThreadLocal.instance().get().getTransaction().lockingStrategy(this);
  }

  @Override
  public void unlock() {
    ODatabaseRecordThreadLocal.instance().get().getTransaction().unlockRecord(this);
  }

  @Override
  public int hashCode() {
    return recordId != null ? recordId.hashCode() : 0;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;

    if (obj instanceof OIdentifiable) return recordId.equals(((OIdentifiable) obj).getIdentity());

    return false;
  }

  public int compare(final OIdentifiable iFirst, final OIdentifiable iSecond) {
    if (iFirst == null || iSecond == null) return -1;
    return iFirst.compareTo(iSecond);
  }

  public int compareTo(final OIdentifiable iOther) {
    if (iOther == null) return 1;

    if (recordId == null) return iOther.getIdentity() == null ? 0 : 1;

    return recordId.compareTo(iOther.getIdentity());
  }

  public ORecordElement.STATUS getInternalStatus() {
    return status;
  }

  public void setInternalStatus(final ORecordElement.STATUS iStatus) {
    this.status = iStatus;
  }

  public ORecordAbstract copyTo(final ORecordAbstract cloned) {
    cloned.source = source;
    cloned.size = size;
    cloned.recordId = recordId.copy();
    cloned.recordVersion = recordVersion;
    cloned.status = status;
    cloned.recordFormat = recordFormat;
    cloned.dirty = false;
    cloned.contentChanged = false;
    cloned.dirtyManager = null;
    return cloned;
  }

  protected ORecordAbstract fill(
      final ORID iRid, final int iVersion, final byte[] iBuffer, boolean iDirty) {
    recordId.setClusterId(iRid.getClusterId());
    recordId.setClusterPosition(iRid.getClusterPosition());
    recordVersion = iVersion;
    status = ORecordElement.STATUS.LOADED;
    source = iBuffer;
    size = iBuffer != null ? iBuffer.length : 0;
    if (source != null && source.length > 0) {
      dirty = iDirty;
      contentChanged = iDirty;
    }

    return this;
  }

  protected ORecordAbstract fill(
      final ORID iRid,
      final int iVersion,
      final byte[] iBuffer,
      boolean iDirty,
      ODatabaseDocumentInternal db) {
    recordId.setClusterId(iRid.getClusterId());
    recordId.setClusterPosition(iRid.getClusterPosition());
    recordVersion = iVersion;
    status = ORecordElement.STATUS.LOADED;
    source = iBuffer;
    size = iBuffer != null ? iBuffer.length : 0;
    if (source != null && source.length > 0) {
      dirty = iDirty;
      contentChanged = iDirty;
    }

    return this;
  }

  protected ORecordAbstract setIdentity(final int iClusterId, final long iClusterPosition) {
    if (recordId == null || recordId == ORecordId.EMPTY_RECORD_ID)
      recordId = new ORecordId(iClusterId, iClusterPosition);
    else {
      recordId.setClusterId(iClusterId);
      recordId.setClusterPosition(iClusterPosition);
    }
    return this;
  }

  protected void unsetDirty() {
    contentChanged = false;
    dirty = false;
  }

  protected abstract byte getRecordType();

  protected void onBeforeIdentityChanged(final ORecord iRecord) {
    if (newIdentityChangeListeners != null) {
      for (OIdentityChangeListener changeListener : newIdentityChangeListeners)
        changeListener.onBeforeIdentityChange(this);
    }
  }

  protected void onAfterIdentityChanged(final ORecord iRecord) {
    if (newIdentityChangeListeners != null) {
      for (OIdentityChangeListener changeListener : newIdentityChangeListeners)
        changeListener.onAfterIdentityChange(this);
    }
  }

  protected ODatabaseDocumentInternal getDatabaseInternal() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  protected ODatabaseDocumentInternal getDatabaseIfDefinedInternal() {
    return ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  protected <RET extends ORecord> RET flatCopy() {
    return copy();
  }

  protected void addIdentityChangeListener(OIdentityChangeListener identityChangeListener) {
    if (newIdentityChangeListeners == null)
      newIdentityChangeListeners =
          Collections.newSetFromMap(new WeakHashMap<OIdentityChangeListener, Boolean>());
    newIdentityChangeListeners.add(identityChangeListener);
  }

  protected void removeIdentityChangeListener(OIdentityChangeListener identityChangeListener) {
    if (newIdentityChangeListeners != null)
      newIdentityChangeListeners.remove(identityChangeListener);
  }

  protected void setup(ODatabaseDocumentInternal db) {
    if (recordId == null) recordId = new ORecordId();
  }

  protected void checkForLoading() {
    if (status == ORecordElement.STATUS.NOT_LOADED
        && ODatabaseRecordThreadLocal.instance().isDefined()) reload(null, true);
  }

  protected boolean isContentChanged() {
    return contentChanged;
  }

  protected void setContentChanged(boolean contentChanged) {
    this.contentChanged = contentChanged;
  }

  protected void clearSource() {
    this.source = null;
  }

  protected ODirtyManager getDirtyManager() {
    if (this.dirtyManager == null) {
      this.dirtyManager = new ODirtyManager();
      if (this.getIdentity().isNew() && getOwner() == null) this.dirtyManager.setDirty(this);
    }
    return this.dirtyManager;
  }

  protected void setDirtyManager(ODirtyManager dirtyManager) {
    if (this.dirtyManager != null && dirtyManager != null) {
      dirtyManager.merge(this.dirtyManager);
    }
    this.dirtyManager = dirtyManager;
    if (this.getIdentity().isNew() && getOwner() == null && this.dirtyManager != null)
      this.dirtyManager.setDirty(this);
  }

  protected void track(OIdentifiable id) {
    this.getDirtyManager().track(this, id);
  }

  protected void unTrack(OIdentifiable id) {
    this.getDirtyManager().unTrack(this, id);
  }
}
