package com.orientechnologies.orient.client.remote.message;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.NETWORK_BINARY_MIN_PROTOCOL_VERSION;
import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.OLDEST_SUPPORTED_PROTOCOL_VERSION;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.config.OConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;

public class OBinaryProtocolHelper {
  private static final OLogger logger = OLogManager.instance().logger(OBinaryProtocolHelper.class);

  public static void checkProtocolVersion(Object caller, int protocolVersion) {

    if (OLDEST_SUPPORTED_PROTOCOL_VERSION > protocolVersion) {
      String message =
          String.format(
              "Backward compatibility support available from to version %d your version is %d",
              OLDEST_SUPPORTED_PROTOCOL_VERSION, protocolVersion);
      logger.error("%s", null, message);
      throw new ODatabaseException(message);
    }

    if (OConfiguration.global().networkBinaryMinProtocolVersion() > protocolVersion) {
      String message =
          String.format(
              "Backward compatibility support enabled from version %d your version is %d, check"
                  + " `%s` settings",
              OConfiguration.global().networkBinaryMinProtocolVersion(),
              protocolVersion,
              NETWORK_BINARY_MIN_PROTOCOL_VERSION.getKey());
      logger.error("%s", null, message);
      throw new ODatabaseException(message);
    }
  }
}
