/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.OShutdownRequest;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import java.io.IOException;
import java.util.Arrays;

/**
 * Sends a shutdown command to the server.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OServerShutdownMain {
  public String networkAddress;
  public int[] networkPort;
  public OChannelBinaryAsynchClient channel;

  private OContextConfiguration contextConfig;
  private String rootUser;
  private String rootPassword;

  public OServerShutdownMain(
      final String iServerAddress,
      final String iServerPorts,
      final String iRootUser,
      final String iRootPassword) {
    contextConfig = new OContextConfiguration();

    rootUser = iRootUser;
    rootPassword = iRootPassword;

    networkAddress = iServerAddress;
    networkPort = OServerNetworkListener.getPorts(iServerPorts);
  }

  public void connect(final int iTimeout) throws IOException {
    // TRY TO CONNECT TO THE RIGHT PORT
    for (int port : networkPort)
      try {
        channel =
            new OChannelBinaryAsynchClient(
                networkAddress,
                port,
                contextConfig,
                OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);
        break;
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on connecting to %s:%d", e, networkAddress, port);
      }

    if (channel == null)
      throw new ONetworkProtocolException(
          "Cannot connect to server host '"
              + networkAddress
              + "', ports: "
              + Arrays.toString(networkPort));

    OShutdownRequest request = new OShutdownRequest(rootUser, rootPassword);
    channel.writeByte(request.getCommand());
    channel.writeInt(0);
    channel.writeBytes(null);
    request.write(channel, null);
    channel.flush();

    channel.beginResponse(0, true);
  }

  public static void main(final String[] iArgs) {

    String serverHost = "localhost";
    String serverPorts = "2424-2430";
    String rootPassword = "NOT_PRESENT";
    String rootUser = OServerConfiguration.DEFAULT_ROOT_USER;

    boolean printUsage = false;

    for (int i = 0; i < iArgs.length; i++) {
      String arg = iArgs[i];
      if ("-P".equals(arg) || "--ports".equals(arg)) serverPorts = iArgs[i + 1];
      if ("-h".equals(arg) || "--host".equals(arg)) serverHost = iArgs[i + 1];
      if ("-p".equals(arg) || "--password".equals(arg)) rootPassword = iArgs[i + 1];
      if ("-u".equals(arg) || "--user".equals(arg)) rootUser = iArgs[i + 1];
      if ("-h".equals(arg) || "--help".equals(arg)) printUsage = true;
    }

    if ("NOT_PRESENT".equals(rootPassword) || printUsage) {
      System.out.println("allowed parameters");
      System.out.println(
          "-h | --host hostname : name or ip of the host where OrientDB is running. Deafult to localhost ");
      System.out.println(
          "-P | --ports  ports : ports in the form of single value or range. Default to 2424-2430");
      System.out.println("-p | --password password : the super user password");
      System.out.println("-u | --user username: the super user name. Default to root");
      System.out.println(
          "example: shutdown.sh -h orientserver.mydomain -P 2424-2430 -u root -p securePassword");
    }

    System.out.println("Sending shutdown command to remote OrientDB Server instance...");

    try {
      new OServerShutdownMain(serverHost, serverPorts, rootUser, rootPassword).connect(5000);
      System.out.println("Shutdown executed correctly");
    } catch (Exception e) {
      System.out.println("Error: " + e.getLocalizedMessage());
    }
    System.out.println();
  }
}
