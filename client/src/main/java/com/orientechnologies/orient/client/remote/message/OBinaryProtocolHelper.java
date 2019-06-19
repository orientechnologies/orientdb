package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.remote.OClusterRemote;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.NETWORK_BINARY_MIN_PROTOCOL_VERSION;
import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.OLDEST_SUPPORTED_PROTOCOL_VERSION;

public class OBinaryProtocolHelper {

  public static void checkProtocolVersion(Object caller, int protocolVersion) {

    if (OLDEST_SUPPORTED_PROTOCOL_VERSION > protocolVersion) {
      String message = String.format("Backward compatibility support available from to version {} your version is {}",
          OLDEST_SUPPORTED_PROTOCOL_VERSION, protocolVersion);
      OLogManager.instance().error(caller, message, null);
      throw new ODatabaseException(message);
    }

    if (NETWORK_BINARY_MIN_PROTOCOL_VERSION.getValueAsInteger() > protocolVersion) {
      String message = String
          .format("Backward compatibility support enabled from version {} your version is {}, check `{}` settings",
              NETWORK_BINARY_MIN_PROTOCOL_VERSION.getValueAsInteger(), protocolVersion,
              NETWORK_BINARY_MIN_PROTOCOL_VERSION.getKey());
      OLogManager.instance().error(caller, message, null);
      throw new ODatabaseException(message);
    }
  }

  public static void writeIdentifiable(OChannelDataOutput channel, final OIdentifiable o, String recordSerializer)
      throws IOException {
    if (o == null)
      channel.writeShort(OChannelBinaryProtocol.RECORD_NULL);
    else if (o instanceof ORecordId) {
      channel.writeShort(OChannelBinaryProtocol.RECORD_RID);
      channel.writeRID((ORID) o);
    } else {
      writeRecord(channel, o.getRecord(), recordSerializer);
    }
  }

  public static void writeRecord(OChannelDataOutput channel, final ORecord iRecord, String recordSerializer) throws IOException {
    channel.writeShort((short) 0);
    channel.writeByte(ORecordInternal.getRecordType(iRecord));
    channel.writeRID(iRecord.getIdentity());
    channel.writeVersion(iRecord.getVersion());
    try {
      final byte[] stream = getRecordBytes(iRecord, recordSerializer);
      channel.writeBytes(stream);
    } catch (Exception e) {
      channel.writeBytes(null);
      final String message = "Error on unmarshalling record " + iRecord.getIdentity().toString() + " (" + e + ")";

      throw OException.wrapException(new OSerializationException(message), e);
    }
  }

  public static byte[] getRecordBytes(final ORecord iRecord, String recordSerializer) {

    final byte[] stream;
    String dbSerializerName = null;
    if (ODatabaseRecordThreadLocal.instance().getIfDefined() != null)
      dbSerializerName = ((ODatabaseDocumentInternal) iRecord.getDatabase()).getSerializer().toString();
    if (ORecordInternal.getRecordType(iRecord) == ODocument.RECORD_TYPE && (dbSerializerName == null || !dbSerializerName
        .equals(recordSerializer))) {
      ((ODocument) iRecord).deserializeFields();
      ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(recordSerializer);
      stream = ser.toStream(iRecord);
    } else
      stream = iRecord.toStream();

    return stream;
  }

  public static Map<UUID, OBonsaiCollectionPointer> readCollectionChanges(OChannelDataInput network) throws IOException {
    Map<UUID, OBonsaiCollectionPointer> collectionsUpdates = new HashMap<>();
    int count = network.readInt();
    for (int i = 0; i < count; i++) {
      final long mBitsOfId = network.readLong();
      final long lBitsOfId = network.readLong();

      final OBonsaiCollectionPointer pointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(network);

      collectionsUpdates.put(new UUID(mBitsOfId, lBitsOfId), pointer);
    }
    return collectionsUpdates;
  }

  public static void writeCollectionChanges(OChannelDataOutput channel, Map<UUID, OBonsaiCollectionPointer> changedIds)
      throws IOException {
    channel.writeInt(changedIds.size());
    for (Entry<UUID, OBonsaiCollectionPointer> entry : changedIds.entrySet()) {
      channel.writeLong(entry.getKey().getMostSignificantBits());
      channel.writeLong(entry.getKey().getLeastSignificantBits());
      OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(channel, entry.getValue());
    }
  }

  public static void writePhysicalPositions(OChannelDataOutput channel, OPhysicalPosition[] previousPositions) throws IOException {
    if (previousPositions == null) {
      channel.writeInt(0); // NO ENTRIEs
    } else {
      channel.writeInt(previousPositions.length);

      for (final OPhysicalPosition physicalPosition : previousPositions) {
        channel.writeLong(physicalPosition.clusterPosition);
        channel.writeInt(physicalPosition.recordSize);
        channel.writeVersion(physicalPosition.recordVersion);
      }
    }
  }

  public static OPhysicalPosition[] readPhysicalPositions(OChannelDataInput network) throws IOException {
    final int positionsCount = network.readInt();
    final OPhysicalPosition[] physicalPositions;
    if (positionsCount == 0) {
      physicalPositions = OCommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
    } else {
      physicalPositions = new OPhysicalPosition[positionsCount];

      for (int i = 0; i < physicalPositions.length; i++) {
        final OPhysicalPosition position = new OPhysicalPosition();

        position.clusterPosition = network.readLong();
        position.recordSize = network.readInt();
        position.recordVersion = network.readVersion();

        physicalPositions[i] = position;
      }
    }
    return physicalPositions;
  }

  public static OCluster[] readClustersArray(final OChannelDataInput network) throws IOException {

    final int tot = network.readShort();
    OCluster[] clusters = new OCluster[tot];
    for (int i = 0; i < tot; ++i) {
      final OClusterRemote cluster = new OClusterRemote();
      String clusterName = network.readString();
      final int clusterId = network.readShort();
      if (clusterName != null) {
        clusterName = clusterName.toLowerCase(Locale.ENGLISH);
        cluster.configure(null, clusterId, clusterName);
        if (clusterId >= clusters.length)
          clusters = Arrays.copyOf(clusters, clusterId + 1);
        clusters[clusterId] = cluster;
      }
    }
    return clusters;
  }

  public static void writeClustersArray(OChannelDataOutput channel, OCluster[] clusters, int protocolVersion) throws IOException {
    int clusterCount = 0;
    for (OCluster c : clusters) {
      if (c != null) {
        ++clusterCount;
      }
    }
    channel.writeShort((short) clusterCount);

    for (OCluster c : clusters) {
      if (c != null) {
        channel.writeString(c.getName());
        channel.writeShort((short) c.getId());
        if (protocolVersion < 24) {
          channel.writeString("none");
          channel.writeShort((short) -1);
        }
      }
    }
  }

}
