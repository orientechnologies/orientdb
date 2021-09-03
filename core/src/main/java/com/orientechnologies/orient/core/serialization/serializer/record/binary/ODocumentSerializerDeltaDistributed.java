package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;

public class ODocumentSerializerDeltaDistributed extends ODocumentSerializerDelta {
  private static ODocumentSerializerDeltaDistributed INSTANCE =
      new ODocumentSerializerDeltaDistributed();

  public static ODocumentSerializerDeltaDistributed instance() {
    return INSTANCE;
  }

  protected void deserializeDeltaLinkBag(BytesContainer bytes, ORidBag toUpdate) {
    boolean isTree = deserializeByte(bytes) == 1;
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED:
          {
            ORecordId link = readOptimizedLink(bytes);
            if (toUpdate != null) {
              toUpdate.add(link);
            }
            break;
          }
        case REPLACED:
          {
            break;
          }
        case REMOVED:
          {
            ORecordId link = readOptimizedLink(bytes);
            if (toUpdate != null) {
              toUpdate.remove(link);
            }
            break;
          }
      }
    }
    if (toUpdate != null) {
      if (isTree) {
        toUpdate.makeTree();
      } else {
        toUpdate.makeEmbedded();
      }
    }
  }

  protected void serializeDeltaLinkBag(BytesContainer bytes, ORidBag value) {
    serializeByte(bytes, value.isEmbedded() ? (byte) 0 : 1);
    OMultiValueChangeTimeLine<OIdentifiable, OIdentifiable> timeline =
        value.getTransactionTimeLine();
    assert timeline != null : "Cx ollection timeline required for link types serialization";
    OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(bytes, event.getValue());
          break;
        case UPDATE:
          throw new UnsupportedOperationException(
              "update do not happen in sets, it will be like and add");
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(bytes, event.getOldValue());
          break;
      }
    }
  }
}
