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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OStreamable;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Base interface for identifiable objects. This abstraction is required to use ORID and ORecord in
 * many points.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OPlaceholder implements OIdentifiable, OStreamable {
  private ORecordId rid;
  private int recordVersion;

  /** Empty constructor used by serialization */
  public OPlaceholder() {}

  public OPlaceholder(final ORecordId rid, final int version) {
    this.rid = rid;
    this.recordVersion = version;
  }

  public OPlaceholder(final ORecord iRecord) {
    rid = (ORecordId) iRecord.getIdentity().copy();
    recordVersion = iRecord.getVersion();
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
    if (!(obj instanceof OPlaceholder)) return false;

    final OPlaceholder other = (OPlaceholder) obj;

    return rid.equals(other.rid) && recordVersion == other.recordVersion;
  }

  @Override
  public int hashCode() {
    return rid.hashCode() + recordVersion;
  }

  @Override
  public int compareTo(OIdentifiable o) {
    return rid.compareTo(o);
  }

  @Override
  public int compare(OIdentifiable o1, OIdentifiable o2) {
    return rid.compare(o1, o2);
  }

  public int getVersion() {
    return recordVersion;
  }

  @Override
  public String toString() {
    return rid.toString() + " v." + recordVersion;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    rid.toStream(out);
    out.writeInt(recordVersion);
  }

  @Override
  public void fromStream(final DataInput in) throws IOException {
    rid = new ORecordId();
    rid.fromStream(in);
    recordVersion = in.readInt();
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
}
