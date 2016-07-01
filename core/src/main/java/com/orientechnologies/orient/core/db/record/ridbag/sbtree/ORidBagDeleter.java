package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.orient.core.db.document.ODocumentFieldVisitor;
import com.orientechnologies.orient.core.db.document.ODocumentFieldWalker;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by tglman on 01/07/16.
 */
public final class ORidBagDeleter implements ODocumentFieldVisitor {

  public static void deleteAllRidBags(ODocument document) {
    final int version = document.getVersion();
    if (document.fields() == 0 && document.getIdentity().isPersistent()) {
      // FORCE LOADING OF CLASS+FIELDS TO USE IT AFTER ON onRecordAfterDelete METHOD
      document.reload();
      if (version > -1 && document.getVersion() != version) // check for record version errors
        if (OFastConcurrentModificationException.enabled())
          throw OFastConcurrentModificationException.instance();
        else
          throw new OConcurrentModificationException(document.getIdentity(), document.getVersion(), version,
              ORecordOperation.DELETED);
    }

    final ODocumentFieldWalker documentFieldWalker = new ODocumentFieldWalker();
    final ORidBagDeleter ridBagDeleter = new ORidBagDeleter();
    documentFieldWalker.walkDocument(document, ridBagDeleter);
  }

  @Override
  public Object visitField(OType type, OType linkedType, Object value) {
    if (value instanceof ORidBag)
      ((ORidBag) value).delete();

    return value;
  }

  @Override
  public boolean goFurther(OType type, OType linkedType, Object value, Object newValue) {
    return true;
  }

  @Override
  public boolean goDeeper(OType type, OType linkedType, Object value) {
    return true;
  }

  @Override
  public boolean updateMode() {
    return false;
  }
}
