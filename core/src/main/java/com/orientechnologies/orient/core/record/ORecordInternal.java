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

package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

public class ORecordInternal {

  /**
   * Internal only. Fills in one shot the record.
   */
  public static ORecordAbstract fill(final ORecord record, final ORID iRid, final int iVersion, final byte[] iBuffer,
      final boolean iDirty) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty);
    return rec;
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static ORecordAbstract setIdentity(final ORecord record, final int iClusterId, final long iClusterPosition) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.setIdentity(iClusterId, iClusterPosition);
    return rec;
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static ORecordAbstract setIdentity(final ORecord record, final ORecordId iIdentity) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.setIdentity(iIdentity);
    return rec;
  }

  /**
   * Internal only. Unsets the dirty status of the record.
   */
  public static void unsetDirty(final ORecord record) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.unsetDirty();
  }

  /**
   * Internal only. Sets the version.
   */
  public static void setVersion(final ORecord record, final int iVersion) {
    final ORecordAbstract rec = (ORecordAbstract) record;
    rec.setVersion(iVersion);
  }

  /**
   * Internal only. Return the record type.
   */
  public static byte getRecordType(final ORecord record) {
    final ORecordAbstract rec = (ORecordAbstract) record;
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

  public static void addIdentityChangeListener(final ORecord record, final OIdentityChangeListener identityChangeListener) {
    ((ORecordAbstract) record).addIdentityChangeListener(identityChangeListener);
  }

  public static void removeIdentityChangeListener(final ORecord record, final OIdentityChangeListener identityChangeListener) {
    ((ORecordAbstract) record).removeIdentityChangeListener(identityChangeListener);
  }

  public static void onBeforeIdentityChanged(final ORecord record) {
    ((ORecordAbstract) record).onBeforeIdentityChanged(record);
  }

  public static void onAfterIdentityChanged(final ORecord record) {
    ((ORecordAbstract) record).onAfterIdentityChanged(record);
  }

  public static void setRecordSerializer(final ORecord record, final ORecordSerializer serializer) {
    ((ORecordAbstract) record)._recordFormat = serializer;
  }

  public static ODirtyManager getDirtyManager(final ORecord record) {
    return ((ORecordAbstract) record).getDirtyManager();
  }

  public static void setDirtyManager(final ORecord record, final ODirtyManager dirtyManager) {
    ((ORecordAbstract) record).setDirtyManager(dirtyManager);
  }

  public static void track(final ORecord pointer, final OIdentifiable pointed) {
    ((ORecordAbstract) pointer).track(pointed);
  }

  public static void unTrack(final ORecord pointer, final OIdentifiable pointed) {
    ((ORecordAbstract) pointer).unTrack(pointed);
  }
}
