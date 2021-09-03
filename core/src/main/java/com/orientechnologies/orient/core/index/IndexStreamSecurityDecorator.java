package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import java.util.stream.Stream;

public class IndexStreamSecurityDecorator {
  public static Stream<ORawPair<Object, ORID>> decorateStream(
      OIndex originalIndex, Stream<ORawPair<Object, ORID>> stream) {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return stream;
    }

    String indexClass = originalIndex.getDefinition().getClassName();
    if (indexClass == null) {
      return stream;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    if (security instanceof OSecurityShared
        && !((OSecurityShared) security).couldHaveActivePredicateSecurityRoles(db, indexClass)) {
      return stream;
    }

    return stream.filter(
        (pair) -> OIndexInternal.securityFilterOnRead(originalIndex, pair.second) != null);
  }

  public static Stream<ORID> decorateRidStream(OIndex originalIndex, Stream<ORID> stream) {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return stream;
    }

    String indexClass = originalIndex.getDefinition().getClassName();
    if (indexClass == null) {
      return stream;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    if (security instanceof OSecurityShared
        && !((OSecurityShared) security).couldHaveActivePredicateSecurityRoles(db, indexClass)) {
      return stream;
    }

    return stream.filter((rid) -> OIndexInternal.securityFilterOnRead(originalIndex, rid) != null);
  }
}
