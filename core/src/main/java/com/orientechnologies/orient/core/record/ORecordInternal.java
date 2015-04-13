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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.version.ORecordVersion;

public class ORecordInternal {

  /**
   * Internal only. Fills in one shot the record.
   */
  public static ORecordAbstract fill(final ORecord record, final ORID iRid, final ORecordVersion iVersion, final byte[] iBuffer,
      final boolean iDirty) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty);
    return rec;
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static ORecordAbstract setIdentity(final ORecord record, final int iClusterId, final long iClusterPosition) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.setIdentity(iClusterId, iClusterPosition);
    return rec;
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static ORecordAbstract setIdentity(final ORecord record, final ORecordId iIdentity) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.setIdentity(iIdentity);
    return rec;
  }

  /**
   * Internal only. Unsets the dirty status of the record.
   */
  public static void unsetDirty(final ORecord record) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.unsetDirty();
  }

  /**
   * Internal only. Sets the version.
   */
  public static void setVersion(final ORecord record, final int iVersion) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.setVersion(iVersion);
  }

  /**
   * Internal only. Return the record type.
   */
  public static byte getRecordType(ORecord record) {
    ORecordAbstract rec = (ORecordAbstract) record;
    return rec.getRecordType();
  }

  public static boolean isContentChanged(ORecord record) {
    ORecordAbstract rec = (ORecordAbstract) record;
    return rec.isContentChanged();
  }

  public static void setContentChanged(ORecord record, boolean changed) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.setContentChanged(changed);
  }

  public static void clearSource(ORecord record) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.clearSource();
  }

  public static void addIdentityChangeListener(ORecord record, OIdentityChangeListener identityChangeListener) {
    ((ORecordAbstract) record).addIdentityChangeListener(identityChangeListener);
  }

  public static void removeIdentityChangeListener(ORecord record, OIdentityChangeListener identityChangeListener) {
    ((ORecordAbstract) record).removeIdentityChangeListener(identityChangeListener);
  }

  public static void onBeforeIdentityChanged(ORecord record) {
    ((ORecordAbstract) record).onBeforeIdentityChanged(record);
  }

  public static void onAfterIdentityChanged(ORecord record) {
    ((ORecordAbstract) record).onAfterIdentityChanged(record);
  }

  /**
   * Internal only. Executes a flat copy of the record.
   */
  public <RET extends ORecord> RET flatCopy(ORecord record) {
    ORecordAbstract rec = (ORecordAbstract) record;
    return rec.flatCopy();
  }

  public static void setRecordSerializer(ORecord record, ORecordSerializer serializer) {
    ((ORecordAbstract) record)._recordFormat = serializer;
  }

}
