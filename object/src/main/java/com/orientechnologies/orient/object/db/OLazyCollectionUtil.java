package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * @author Wouter de Vaal
 */
public class OLazyCollectionUtil {

  private OLazyCollectionUtil(){
  }
	
    /**
     * Gets the current thread database as a ODatabasePojoAbstract, wrapping it where necessary.
     */
  protected static <TYPE> ODatabasePojoAbstract<TYPE> getDatabase() {
    ODatabaseInternal<?> databaseOwner = ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
    if (databaseOwner instanceof ODatabasePojoAbstract) {
      return (ODatabasePojoAbstract<TYPE>) databaseOwner;
    } else if (databaseOwner instanceof ODatabaseDocumentInternal) {
      return (ODatabasePojoAbstract<TYPE>) new OObjectDatabaseTx((ODatabaseDocumentInternal) databaseOwner);
    }
    throw new IllegalStateException("Current database not of expected type");
  }
}
