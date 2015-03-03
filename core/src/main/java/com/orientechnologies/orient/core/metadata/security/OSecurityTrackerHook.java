package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

import java.lang.ref.WeakReference;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 04/11/14
 */
public class OSecurityTrackerHook extends ODocumentHookAbstract {
  private final WeakReference<OSecurity> security;

  public OSecurityTrackerHook(OSecurity security, ODatabaseDocument database) {
    super(database);
    this.security = new WeakReference<OSecurity>(security);
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public void onRecordAfterCreate(ODocument doc) {
    incrementSchemaVersion(doc);
  }

  @Override
  public void onRecordAfterUpdate(ODocument doc) {
    incrementSchemaVersion(doc);
  }

  @Override
  public void onRecordAfterDelete(ODocument doc) {
    incrementSchemaVersion(doc);
  }

  @Override
  public void onRecordCreateReplicated(ODocument doc) {
    incrementSchemaVersion(doc);
  }

  @Override
  public void onRecordUpdateReplicated(ODocument doc) {
    incrementSchemaVersion(doc);
  }

  @Override
  public void onRecordDeleteReplicated(ODocument doc) {
    incrementSchemaVersion(doc);
  }

  private void incrementSchemaVersion(ODocument doc) {
    if (ODocumentInternal.getImmutableSchemaClass(doc) == null)
      return;

    final String className = ODocumentInternal.getImmutableSchemaClass(doc).getName();

    if (className.equalsIgnoreCase(OUser.CLASS_NAME) || className.equalsIgnoreCase(ORole.CLASS_NAME)) {
      final OSecurity scr = security.get();
      if (scr != null)
        scr.incrementVersion();
    }
  }
}
