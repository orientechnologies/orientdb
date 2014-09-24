package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.version.ORecordVersion;

public class ORecordInternal {

  /**
   * Internal only. Fills in one shot the record.
   */
  public static ORecordAbstract fill(ORecord record, ORID iRid, ORecordVersion iVersion, byte[] iBuffer, boolean iDirty) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty);
    return rec;
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static ORecordAbstract setIdentity(ORecord record, int iClusterId, OClusterPosition iClusterPosition) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.setIdentity(iClusterId, iClusterPosition);
    return rec;
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static ORecordAbstract setIdentity(ORecord record, ORecordId iIdentity) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.setIdentity(iIdentity);
    return rec;
  }

  /**
   * Internal only. Unsets the dirty status of the record.
   */
  public static void unsetDirty(ORecord record) {
    ORecordAbstract rec = (ORecordAbstract) record;
    rec.unsetDirty();
  }

  /**
   * Internal only. Sets the version.
   */
  public static void setVersion(ORecord record, int iVersion) {
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

  /**
   * Internal only. Executes a flat copy of the record.
   * 
   * @see #copy()
   */
  public <RET extends ORecord> RET flatCopy(ORecord record) {
    ORecordAbstract rec = (ORecordAbstract) record;
    return rec.flatCopy();
  }

}
