package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;

/** This class is designed to compare documents based on deep equality (to be used in Sets) */
public class ODocumentEqualityWrapper {
  private final ODocument internal;

  ODocumentEqualityWrapper(ODocument internal) {

    this.internal = internal;
  }

  public boolean equals(Object obj) {
    if (obj instanceof ODocumentEqualityWrapper) {
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      return ODocumentHelper.hasSameContentOf(
          internal, db, ((ODocumentEqualityWrapper) obj).internal, db, null);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 0;
    for (String fieldName : internal.fieldNames()) {
      result += fieldName.hashCode();
      Object value = internal.field(fieldName);
      if (value != null) {
        result += value.hashCode();
      }
    }
    return result;
  }
}
