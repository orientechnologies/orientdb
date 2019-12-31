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

}
