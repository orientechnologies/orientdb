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
