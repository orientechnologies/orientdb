package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;

/** @author Wouter de Vaal */
public class OLazyCollectionUtil {

  /** Gets the current thread database as a ODatabasePojoAbstract, wrapping it where necessary. */
  protected static OObjectDatabaseTx getDatabase() {
    ODatabaseInternal<?> databaseOwner =
        ODatabaseRecordThreadLocal.instance().get().getDatabaseOwner();
    if (databaseOwner instanceof OObjectDatabaseTx) {
      return (OObjectDatabaseTx) databaseOwner;
    } else if (databaseOwner instanceof ODatabaseDocumentInternal) {
      return new OObjectDatabaseTx((ODatabaseDocumentInternal) databaseOwner);
    }
    throw new IllegalStateException("Current database not of expected type");
  }
}
