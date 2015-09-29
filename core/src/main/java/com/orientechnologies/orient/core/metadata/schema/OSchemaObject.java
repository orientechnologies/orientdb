package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.annotation.OAfterSerialization;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Interface for a class implementing a view schema object
 *
 * @author Matan Shukry
 * @since 29/9/2015
 */
public abstract class OSchemaObject extends ODocumentWrapperNoClass {
  private int                             hashCode;
  protected final OSchemaShared           owner;

  /* Constructor */
  protected OSchemaObject(OSchemaShared owner) {
    this(owner, new ODocument().setTrackingChanges(true));
  }

  protected OSchemaObject(OSchemaShared owner, ODocument document) {
    this.owner = owner;
    this.document = document;
  }

  /* Pure virtual methods */
  protected abstract int calculateHashCode(int baseHashCode);
  protected abstract void saveDocument();
  protected abstract void loadDocument();

  /* Properties */
  public OSchemaShared getOwner() {
    return owner;
  }

  /* Helper methods (mostly for parents */
  protected void acquireSchemaReadLock() {
    owner.acquireSchemaReadLock();
  }

  protected void releaseSchemaReadLock() {
    owner.releaseSchemaReadLock();
  }

  protected void acquireSchemaWriteLock() {
    owner.acquireSchemaWriteLock();
  }

  protected void releaseSchemaWriteLock() {
    releaseSchemaWriteLock(true);
  }

  protected void releaseSchemaWriteLock(final boolean iSave) {
    _calculateHashCode();
    owner.releaseSchemaWriteLock(iSave);
  }

  /* */
  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  protected boolean isDistributedCommand() {
    return getDatabase().getStorage() instanceof OAutoshardedStorage
      && OScenarioThreadLocal.INSTANCE.get() != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED;
  }

  protected void checkEmbedded() {
    owner.checkEmbedded(getDatabase().getStorage().getUnderlying().getUnderlying());
  }

  @Override
  @OAfterSerialization  /* Don't think it's needed, not used besides import and annotations */
  protected void fromStream() {
    loadDocument();
  }

  @Override
  @OBeforeSerialization /* Don't think it's needed, not used besides import and annotations */
  public ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

    try {
      saveDocument();
    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }

    return document;
  }

  /* Hash Code */
  private void _calculateHashCode() {
    this.hashCode = this.calculateHashCode(super.hashCode());
  }

  @Override
  public int hashCode() {
    int sh = hashCode;
    if (sh != 0)
      return sh;

    acquireSchemaReadLock();
    try {
      sh = hashCode;
      if (sh != 0)
        return sh;

      _calculateHashCode();
      return hashCode;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public ORID getDocumentIdentity() {
    return this.document.getIdentity();
  }
}
