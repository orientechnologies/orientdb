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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Base interface for identifiable objects. This abstraction is required to use ORID and ORecord in many points.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OPlaceholder implements OIdentifiable, Externalizable {
  private ORecordId      rid;
  private ORecordVersion recordVersion;

  /**
   * Empty constructor used by serialization
   */
  public OPlaceholder() {
  }

  public OPlaceholder(final ORecordId rid, final ORecordVersion version) {
    this.rid = rid;
    this.recordVersion = version;
  }

  public OPlaceholder(final ORecord iRecord) {
    rid = (ORecordId) iRecord.getIdentity().copy();
    recordVersion = iRecord.getRecordVersion().copy();
  }

  @Override
  public ORID getIdentity() {
    return rid;
  }

  @Override
  public <T extends ORecord> T getRecord() {
    return rid.getRecord();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof OPlaceholder))
      return false;

    final OPlaceholder other = (OPlaceholder) obj;

    return rid.equals(other.rid) && recordVersion.equals(other.recordVersion);
  }

  @Override
  public int hashCode() {
    return rid.hashCode() + recordVersion.hashCode();
  }

  @Override
  public int compareTo(OIdentifiable o) {
    return rid.compareTo(o);
  }

  @Override
  public int compare(OIdentifiable o1, OIdentifiable o2) {
    return rid.compare(o1, o2);
  }

  public ORecordVersion getRecordVersion() {
    return recordVersion;
  }

  @Override
  public String toString() {
    return rid.toString() + " v." + recordVersion.toString();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(rid);
    out.writeObject(recordVersion);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    rid = (ORecordId) in.readObject();
    recordVersion = (ORecordVersion) in.readObject();
  }

  @Override
  public void lock(final boolean iExclusive) {
    ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction()
        .lockRecord(this, iExclusive ? OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK : OStorage.LOCKING_STRATEGY.SHARED_LOCK);
  }

  @Override
  public boolean isLocked() {
    return ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().isLockedRecord(this);
  }

  @Override
  public void unlock() {
    ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().unlockRecord(this);
  }
}
