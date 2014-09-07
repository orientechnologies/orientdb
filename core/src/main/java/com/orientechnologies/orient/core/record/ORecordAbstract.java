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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

@SuppressWarnings({ "unchecked", "serial" })
public abstract class ORecordAbstract<T> implements ORecord<T>, ORecordInternal<T> {
  protected ORecordId                            _recordId;
  protected ORecordVersion                       _recordVersion          = OVersionFactory.instance().createVersion();

  protected byte[]                               _source;
  protected int                                  _size;
  protected String                               _dataSegmentName;
  protected transient ORecordSerializer          _recordFormat;
  protected Boolean                              _pinned                 = null;
  protected boolean                              _dirty                  = true;
  protected ORecordElement.STATUS                _status                 = ORecordElement.STATUS.LOADED;
  protected transient Set<ORecordListener>       _listeners              = null;

  private ORID                                   prevRid                 = null;
  private transient Set<OIdentityChangeListener> identityChangeListeners = Collections
                                                                             .newSetFromMap(new WeakHashMap<OIdentityChangeListener, Boolean>());

  public ORecordAbstract() {
  }

  public ORecordAbstract(final byte[] iSource) {
    _source = iSource;
    _size = iSource.length;
    unsetDirty();
  }

  public ORecordAbstract<?> fill(final ORID iRid, final ORecordVersion iVersion, final byte[] iBuffer, boolean iDirty) {
    _recordId.clusterId = iRid.getClusterId();
    _recordId.clusterPosition = iRid.getClusterPosition();
    _recordVersion.copyFrom(iVersion);
    _status = ORecordElement.STATUS.LOADED;
    _source = iBuffer;
    _size = iBuffer != null ? iBuffer.length : 0;
    if (_source != null && _source.length > 0)
      _dirty = iDirty;

    return this;
  }

  public ORID getIdentity() {
    return _recordId;
  }

  public ORecordAbstract<?> setIdentity(final ORecordId iIdentity) {
    _recordId = iIdentity;
    return this;
  }

  @Override
  public ORecordElement getOwner() {
    return null;
  }

  public ORecord<?> getRecord() {
    return this;
  }

  public ORecordAbstract<?> setIdentity(final int iClusterId, final OClusterPosition iClusterPosition) {
    if (_recordId == null || _recordId == ORecordId.EMPTY_RECORD_ID)
      _recordId = new ORecordId(iClusterId, iClusterPosition);
    else {
      _recordId.clusterId = iClusterId;
      _recordId.clusterPosition = iClusterPosition;
    }
    return this;
  }

  public boolean detach() {
    return true;
  }

  public ORecordAbstract<T> clear() {
    setDirty();
    invokeListenerEvent(ORecordListener.EVENT.CLEAR);
    return this;
  }

  public ORecordAbstract<T> reset() {
    _status = ORecordElement.STATUS.LOADED;
    _recordVersion.reset();
    _size = 0;

    _source = null;
    setDirty();
    if (_recordId != null)
      _recordId.reset();

    invokeListenerEvent(ORecordListener.EVENT.RESET);

    return this;
  }

  public byte[] toStream() {
    if (_source == null)
      _source = _recordFormat.toStream(this, false);

    invokeListenerEvent(ORecordListener.EVENT.MARSHALL);

    return _source;
  }

  public ORecordAbstract<T> fromStream(final byte[] iRecordBuffer) {
    _dirty = false;
    _source = iRecordBuffer;
    _size = iRecordBuffer != null ? iRecordBuffer.length : 0;
    _status = ORecordElement.STATUS.LOADED;

    invokeListenerEvent(ORecordListener.EVENT.UNMARSHALL);

    return this;
  }

  public void unsetDirty() {
    if (_dirty)
      _dirty = false;
  }

  public ORecordAbstract<T> setDirty() {
    if (!_dirty && _status != STATUS.UNMARSHALLING) {
      _dirty = true;
      _source = null;
    }
    return this;
  }

  public void onBeforeIdentityChanged(final ORID iRID) {
    prevRid = _recordId.copy();
  }

  public void onAfterIdentityChanged(final ORecord<?> iRecord) {
    invokeListenerEvent(ORecordListener.EVENT.IDENTITY_CHANGED);

    if (!prevRid.equals(this._recordId)) {
      for (OIdentityChangeListener changeListener : identityChangeListeners)
        changeListener.onIdentityChanged(prevRid, this);
    }

    prevRid = null;
  }

  public boolean isDirty() {
    return _dirty;
  }

  public Boolean isPinned() {
    return _pinned;
  }

  public ORecordAbstract<T> pin() {
    _pinned = Boolean.TRUE;
    return this;
  }

  public ORecordAbstract<T> unpin() {
    _pinned = Boolean.FALSE;
    return this;
  }

  public <RET extends ORecord<T>> RET fromJSON(final String iSource, final String iOptions) {
    // ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null, iOptions);
    ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null, iOptions, false); // Add new parameter to accommodate new API,
                                                                                     // nothing change
    return (RET) this;
  }

  public <RET extends ORecord<T>> RET fromJSON(final String iSource) {
    ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null);
    return (RET) this;
  }

  // Add New API to load record if rid exist
  public <RET extends ORecord<T>> RET fromJSON(final String iSource, boolean needReload) {
    return (RET) ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null, needReload);
  }

  public <RET extends ORecord<T>> RET fromJSON(final InputStream iContentResult) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    OIOUtils.copyStream(iContentResult, out, -1);
    ORecordSerializerJSON.INSTANCE.fromString(out.toString(), this, null);
    return (RET) this;
  }

  public String toJSON() {
    return toJSON("rid,version,class,type,attribSameRow,keepTypes,alwaysFetchEmbedded,fetchPlan:*:0");
  }

  public String toJSON(final String iFormat) {
    return ORecordSerializerJSON.INSTANCE.toString(this, new StringBuilder(), iFormat).toString();
  }

  @Override
  public String toString() {
    return (_recordId.isValid() ? _recordId : "") + (_source != null ? Arrays.toString(_source) : "[]") + " v"
        + _recordVersion.toString();
  }

  public String getDataSegmentName() {
    return _dataSegmentName;
  }

  public <RET extends ORecord<T>> RET setDataSegmentName(final String iName) {
    if (_recordId.isValid())
      throw new IllegalStateException("Cannot assign a data segment to a not new record");

    _dataSegmentName = iName;
    return (RET) this;
  }

  public int getVersion() {
    checkForLoading();
    return _recordVersion.getCounter();
  }

  public void setVersion(final int iVersion) {
    _recordVersion.setCounter(iVersion);
  }

  public ORecordVersion getRecordVersion() {
    checkForLoading();
    return _recordVersion;
  }

  public ORecordAbstract<T> unload() {
    _status = ORecordElement.STATUS.NOT_LOADED;
    _source = null;
    unsetDirty();
    invokeListenerEvent(ORecordListener.EVENT.UNLOAD);
    return this;
  }

  public ORecordInternal<T> load() {
    if (!getIdentity().isValid())
      throw new ORecordNotFoundException("The record has no id, probably it's new or transient yet ");

    try {
      final ORecordInternal<?> result = getDatabase().load(this);

      if (result == null)
        throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' not found");

      return (ORecordInternal<T>) result;
    } catch (Exception e) {
      throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' not found", e);
    }
  }

  public ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public ODatabaseRecord getDatabaseIfDefined() {
    return ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
  }

  public ORecordInternal<T> reload() {
    return reload(null);
  }

  public ORecordInternal<T> reload(final String iFetchPlan) {
    return reload(null, true);
  }

  public ORecordInternal<T> reload(final String iFetchPlan, final boolean iIgnoreCache) {
    if (!getIdentity().isValid())
      throw new ORecordNotFoundException("The record has no id. It is probably new or still transient");

    try {
      getDatabase().reload(this, iFetchPlan, iIgnoreCache);

      // GET CONTENT
      // fromStream(toStream());

      return this;
    } catch (Exception e) {
      throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' not found", e);
    }
  }

  public ORecordAbstract<T> save() {
    return save(false);
  }

  public ORecordAbstract<T> save(final String iClusterName) {
    return save(iClusterName, false);
  }

  public ORecordAbstract<T> save(boolean forceCreate) {
    getDatabase().save(this, ODatabaseComplex.OPERATION_MODE.SYNCHRONOUS, forceCreate, null, null);
    return this;
  }

  public ORecordAbstract<T> save(String iClusterName, boolean forceCreate) {
    getDatabase().save(this, iClusterName, ODatabaseComplex.OPERATION_MODE.SYNCHRONOUS, forceCreate, null, null);
    return this;
  }

  public ORecordAbstract<T> delete() {
    getDatabase().delete(this);
    setDirty();
    return this;
  }

  public int getSize() {
    return _size;
  }

  @Override
  public void lock(final boolean iExclusive) {
    ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction()
        .lockRecord(this, iExclusive ? OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK : OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK);
  }

  @Override
  public void unlock() {
    ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().unlockRecord(this);
  }

  @Override
  public int hashCode() {
    return _recordId != null ? _recordId.hashCode() : 0;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;

    if (obj instanceof OIdentifiable)
      return _recordId.equals(((OIdentifiable) obj).getIdentity());

    return false;
  }

  public int compare(final OIdentifiable iFirst, final OIdentifiable iSecond) {
    if (iFirst == null || iSecond == null)
      return -1;
    return iFirst.compareTo(iSecond);
  }

  public int compareTo(final OIdentifiable iOther) {
    if (iOther == null)
      return 1;

    if (_recordId == null)
      return iOther.getIdentity() == null ? 0 : 1;

    return _recordId.compareTo(iOther.getIdentity());
  }

  public ORecordElement.STATUS getInternalStatus() {
    return _status;
  }

  public void setInternalStatus(final ORecordElement.STATUS iStatus) {
    this._status = iStatus;
  }

  public ORecordAbstract<T> copyTo(final ORecordAbstract<T> cloned) {
    cloned._source = _source;
    cloned._size = _size;
    cloned._recordId = _recordId.copy();
    cloned._recordVersion = _recordVersion.copy();
    cloned._pinned = _pinned;
    cloned._status = _status;
    cloned._recordFormat = _recordFormat;
    cloned._listeners = null;
    cloned._dirty = false;
    return cloned;
  }

  /**
   * Add a listener to the current document to catch all the supported events.
   * 
   * @see ORecordListener
   * 
   * @param iListener
   *          ODocumentListener implementation
   */
  public void addListener(final ORecordListener iListener) {
    if (_listeners == null)
      _listeners = Collections.newSetFromMap(new WeakHashMap<ORecordListener, Boolean>());

    _listeners.add(iListener);
  }

  /**
   * Remove the current event listener.
   * 
   * @see ORecordListener
   */
  public void removeListener(final ORecordListener listener) {
    if (_listeners != null) {
      _listeners.remove(listener);
      if (_listeners.isEmpty())
        _listeners = null;
    }
  }

  public <RET extends ORecord<T>> RET flatCopy() {
    return (RET) copy();
  }

  @Override
  public void addIdentityChangeListener(OIdentityChangeListener identityChangeListener) {
    identityChangeListeners.add(identityChangeListener);
  }

  @Override
  public void removeIdentityChangeListener(OIdentityChangeListener identityChangeListener) {
    identityChangeListeners.remove(identityChangeListener);
  }

  protected void setup() {
    if (_recordId == null)
      _recordId = new ORecordId();
  }

  protected void invokeListenerEvent(final ORecordListener.EVENT iEvent) {
    if (_listeners != null)
      for (final ORecordListener listener : _listeners)
        if (listener != null)
          listener.onEvent(this, iEvent);
  }

  protected void checkForLoading() {
    if (_status == ORecordElement.STATUS.NOT_LOADED && ODatabaseRecordThreadLocal.INSTANCE.isDefined())
      reload(null, true);
  }
}
