package com.orientechnologies.orient.client.remote.message;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.NETWORK_BINARY_MIN_PROTOCOL_VERSION;
import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.OLDEST_SUPPORTED_PROTOCOL_VERSION;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;

public class OBinaryProtocolHelper {

  public static void checkProtocolVersion(Object caller, int protocolVersion) {

    if (OLDEST_SUPPORTED_PROTOCOL_VERSION > protocolVersion) {
      String message =
          String.format(
              "Backward compatibility support available from to version {} your version is {}",
              OLDEST_SUPPORTED_PROTOCOL_VERSION,
              protocolVersion);
      OLogManager.instance().error(caller, message, null);
      throw new ODatabaseException(message);
    }

    if (NETWORK_BINARY_MIN_PROTOCOL_VERSION.getValueAsInteger() > protocolVersion) {
      String message =
          String.format(
              "Backward compatibility support enabled from version {} your version is {}, check `{}` settings",
              NETWORK_BINARY_MIN_PROTOCOL_VERSION.getValueAsInteger(),
              protocolVersion,
              NETWORK_BINARY_MIN_PROTOCOL_VERSION.getKey());
      OLogManager.instance().error(caller, message, null);
      throw new ODatabaseException(message);
    }
  }
}
