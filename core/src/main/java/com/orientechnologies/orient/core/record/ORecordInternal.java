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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

public class ORecordInternal {

  /** Internal only. Fills in one shot the record. */
  public static ORecordAbstract fill(
      final ORecord record,
      final ORID iRid,
      final int iVersion,
      final byte[] iBuffer,
      final boolean iDirty) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty);
    return rec;
  }

  public static ORecordAbstract fill(
      final ORecord record,
      final ORID iRid,
      final int iVersion,
      final byte[] iBuffer,
      final boolean iDirty,
      ODatabaseDocumentInternal db) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty, db);
    return rec;
  }

  public static void fromStream(
      final ORecord record, final byte[] iBuffer, ODatabaseDocumentInternal db) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.fromStream(iBuffer, db);
  }

  /** Internal only. Changes the identity of the record. */
  public static ORecordAbstract setIdentity(
      final ORecord record, final int iClusterId, final long iClusterPosition) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.setIdentity(iClusterId, iClusterPosition);
    return rec;
  }

  /** Internal only. Changes the identity of the record. */
  public static ORecordAbstract setIdentity(final ORecord record, final ORecordId iIdentity) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.setIdentity(iIdentity);
    return rec;
  }

  /** Internal only. Unsets the dirty status of the record. */
  public static void unsetDirty(final ORecord record) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.unsetDirty();
  }

  /** Internal only. Sets the version. */
  public static void setVersion(final ORecord record, final int iVersion) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.setVersion(iVersion);
  }

  /** Internal only. Return the record type. */
  public static byte getRecordType(final ORecord record) {
    if (record instanceof ORecordAbstract) {
      return ((ORecordAbstract) record).getRecordType();
    }
    final ORecordAbstract rec = (ORecordAbstract) record.getRecord();
    return rec.getRecordType();
  }

  public static boolean isContentChanged(final ORecord record) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    return rec.isContentChanged();
  }

  public static void setContentChanged(final ORecord record, final boolean changed) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.setContentChanged(changed);
  }

  public static void clearSource(final ORecord record) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.clearSource();
  }

  public static void addIdentityChangeListener(
      ORecord record, final OIdentityChangeListener identityChangeListener) {
    if (!(record instanceof ORecordAbstract)) {
      // manage O*Delegate
      record = record.getRecord();
    }
    if (record instanceof ORecordAbstract) {
      ((ORecordAbstract) record).addIdentityChangeListener(identityChangeListener);
    }
  }

  public static void removeIdentityChangeListener(
      final ORecord record, final OIdentityChangeListener identityChangeListener) {
    ((ORecordAbstract) record).removeIdentityChangeListener(identityChangeListener);
  }

  public static void onBeforeIdentityChanged(final ORecord record) {
    ((ORecordAbstract) record).onBeforeIdentityChanged(record);
  }

  public static void onAfterIdentityChanged(final ORecord record) {
    ((ORecordAbstract) record).onAfterIdentityChanged(record);
  }

  public static void setRecordSerializer(final ORecord record, final ORecordSerializer serializer) {
    ((ORecordAbstract) record).recordFormat = serializer;
  }

  public static ODirtyManager getDirtyManager(ORecord record) {
    if (!(record instanceof ORecordAbstract)) {
      record = record.getRecord();
    }
    return ((ORecordAbstract) record).getDirtyManager();
  }

  public static void setDirtyManager(ORecord record, final ODirtyManager dirtyManager) {
    if (!(record instanceof ORecordAbstract)) {
      record = record.getRecord();
    }
    ((ORecordAbstract) record).setDirtyManager(dirtyManager);
  }

  public static void track(final ORecordElement pointer, final OIdentifiable pointed) {
    ORecordElement firstRecord = pointer;
    while (firstRecord != null && !(firstRecord instanceof ORecord)) {
      firstRecord = firstRecord.getOwner();
    }
    if (firstRecord instanceof ORecordAbstract) {
      ((ORecordAbstract) firstRecord).track(pointed);
    }
  }

  public static void unTrack(final ORecordElement pointer, final OIdentifiable pointed) {
    ORecordElement firstRecord = pointer;
    while (firstRecord != null && !(firstRecord instanceof ORecord)) {
      firstRecord = firstRecord.getOwner();
    }
    if (firstRecord instanceof ORecordAbstract) {
      ((ORecordAbstract) firstRecord).unTrack(pointed);
    }
  }

  public static ORecordSerializer getRecordSerializer(ORecord iRecord) {
    return ((ORecordAbstract) iRecord).recordFormat;
  }
}
