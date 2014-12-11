package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * @author Wouter de Vaal
 */
public class OLazyCollectionUtil {

    /**
     * Gets the current thread database as a ODatabasePojoAbstract, wrapping it where necessary.
     */
  protected static <TYPE> ODatabasePojoAbstract<TYPE> getDatabase() {
    ODatabaseInternal<?> databaseOwner = ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
    if (databaseOwner instanceof ODatabasePojoAbstract) {
      return (ODatabasePojoAbstract<TYPE>) databaseOwner;
    } else if (databaseOwner instanceof ODatabaseDocumentTx) {
      return (ODatabasePojoAbstract<TYPE>) new OObjectDatabaseTx((ODatabaseDocumentTx) databaseOwner);
    }
    throw new IllegalStateException("Current database not of expected type");
  }
}
