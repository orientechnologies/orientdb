package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import java.util.Collection;
import java.util.Map;

public class ORecordSerializerNetworkDistributed extends ORecordSerializerNetworkV37 {

  public static final ORecordSerializerNetworkDistributed INSTANCE =
      new ORecordSerializerNetworkDistributed();

  protected int writeOptimizedLink(final BytesContainer bytes, OIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      final ORecord real = link.getRecord();
      if (real != null) link = real;
    }

    if (!link.getIdentity().isPersistent() && !link.getIdentity().isTemporary()) {
      throw new ODatabaseException(
          "Found not persistent link with no connected record, probably missing save `"
              + link.getIdentity()
              + "` ");
    }
    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    return pos;
  }

  protected Collection<Map.Entry<String, ODocumentEntry>> fetchEntries(ODocument document) {
    return ODocumentInternal.rawEntries(document);
  }
}
