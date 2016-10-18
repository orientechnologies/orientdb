package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OFetchPlanResults;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.SimpleValueFetchPlanCommandListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OCommandResponseServer implements OBinaryResponse<Object> {

  private Object                              result;
  private SimpleValueFetchPlanCommandListener listener;
  private boolean                             isRecordResultSet;

  public OCommandResponseServer(Object result, SimpleValueFetchPlanCommandListener listener, boolean isRecordResultSet) {
    this.result = result;
    this.listener = listener;
    this.isRecordResultSet = isRecordResultSet;
  }

  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    serializeValue(channel, listener, result, false, isRecordResultSet, protocolVersion, recordSerializer);

    if (listener instanceof OFetchPlanResults) {
      // SEND FETCHED RECORDS TO LOAD IN CLIENT CACHE
      for (ORecord rec : ((OFetchPlanResults) listener).getFetchedRecordsToSend()) {
        channel.writeByte((byte) 2); // CLIENT CACHE RECORD. IT
        // ISN'T PART OF THE
        // RESULT SET
        OBinaryProtocolHelper.writeIdentifiable(channel, rec, recordSerializer);
      }

      channel.writeByte((byte) 0); // NO MORE RECORDS
    }

  }

  public void serializeValue(OChannelBinary channel, final SimpleValueFetchPlanCommandListener listener, Object result,
      boolean load, boolean isRecordResultSet, int protocolVersion, String recordSerializer) throws IOException {
    if (result == null) {
      // NULL VALUE
      channel.writeByte((byte) 'n');
    } else if (result instanceof OIdentifiable) {
      // RECORD
      channel.writeByte((byte) 'r');
      if (load && result instanceof ORecordId)
        result = ((ORecordId) result).getRecord();

      if (listener != null)
        listener.result(result);
      OBinaryProtocolHelper.writeIdentifiable(channel, (OIdentifiable) result, recordSerializer);
    } else if (result instanceof ODocumentWrapper) {
      // RECORD
      channel.writeByte((byte) 'r');
      final ODocument doc = ((ODocumentWrapper) result).getDocument();
      if (listener != null)
        listener.result(doc);
      OBinaryProtocolHelper.writeIdentifiable(channel, doc, recordSerializer);
    } else if (!isRecordResultSet) {
      writeSimpleValue(channel, listener, result, protocolVersion, recordSerializer);
    } else if (OMultiValue.isMultiValue(result)) {
      final byte collectionType = result instanceof Set ? (byte) 's' : (byte) 'l';
      channel.writeByte(collectionType);
      channel.writeInt(OMultiValue.getSize(result));
      for (Object o : OMultiValue.getMultiValueIterable(result, false)) {
        try {
          if (load && o instanceof ORecordId)
            o = ((ORecordId) o).getRecord();
          if (listener != null)
            listener.result(o);

          OBinaryProtocolHelper.writeIdentifiable(channel, (OIdentifiable) o, recordSerializer);
        } catch (Exception e) {
          OLogManager.instance().warn(this, "Cannot serialize record: " + o);
          OBinaryProtocolHelper.writeIdentifiable(channel, null, recordSerializer);
          // WRITE NULL RECORD TO AVOID BREAKING PROTOCOL
        }
      }
    } else if (OMultiValue.isIterable(result)) {
      if (protocolVersion >= OChannelBinaryProtocol.PROTOCOL_VERSION_32) {
        channel.writeByte((byte) 'i');
        for (Object o : OMultiValue.getMultiValueIterable(result)) {
          try {
            if (load && o instanceof ORecordId)
              o = ((ORecordId) o).getRecord();
            if (listener != null)
              listener.result(o);

            channel.writeByte((byte) 1); // ONE MORE RECORD
            OBinaryProtocolHelper.writeIdentifiable(channel, (OIdentifiable) o, recordSerializer);
          } catch (Exception e) {
            OLogManager.instance().warn(this, "Cannot serialize record: " + o);
          }
        }
        channel.writeByte((byte) 0); // NO MORE RECORD
      } else {
        // OLD RELEASES: TRANSFORM IN A COLLECTION
        final byte collectionType = result instanceof Set ? (byte) 's' : (byte) 'l';
        channel.writeByte(collectionType);
        channel.writeInt(OMultiValue.getSize(result));
        for (Object o : OMultiValue.getMultiValueIterable(result)) {
          try {
            if (load && o instanceof ORecordId)
              o = ((ORecordId) o).getRecord();
            if (listener != null)
              listener.result(o);

            OBinaryProtocolHelper.writeIdentifiable(channel, (OIdentifiable) o, recordSerializer);
          } catch (Exception e) {
            OLogManager.instance().warn(this, "Cannot serialize record: " + o);
          }
        }
      }

    } else {
      // ANY OTHER (INCLUDING LITERALS)
      writeSimpleValue(channel, listener, result, protocolVersion, recordSerializer);
    }
  }

  private void writeSimpleValue(OChannelBinary channel, SimpleValueFetchPlanCommandListener listener, Object result,
      int protocolVersion, String recordSerializer) throws IOException {

    if (protocolVersion >= OChannelBinaryProtocol.PROTOCOL_VERSION_35) {
      channel.writeByte((byte) 'w');
      ODocument document = new ODocument();
      document.field("result", result);
      OBinaryProtocolHelper.writeIdentifiable(channel, document, recordSerializer);
      if (listener != null)
        listener.linkdedBySimpleValue(document);
    } else {
      channel.writeByte((byte) 'a');
      final StringBuilder value = new StringBuilder(64);
      if (listener != null) {
        ODocument document = new ODocument();
        document.field("result", result);
        listener.linkdedBySimpleValue(document);
      }
      ORecordSerializerStringAbstract.fieldTypeToString(value, OType.getTypeByClass(result.getClass()), result);
      channel.writeString(value.toString());
    }
  }

  @Override
  public Object read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    throw new UnsupportedOperationException();
  }

}
