/*
      *
      *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
      *  * For more information: http://www.orientechnologies.com
      *
      */
package com.orientechnologies.orient.server;

import com.orientechnologies.common.log.OLogManager;
 import com.orientechnologies.orient.client.db.ODatabaseHelper;
 import com.orientechnologies.orient.core.config.OContextConfiguration;
 import com.orientechnologies.orient.core.exception.OConfigurationException;
 import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
 import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClientSynch;
 import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
 import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
 import com.orientechnologies.orient.server.config.OServerConfiguration;
 import com.orientechnologies.orient.server.config.OServerConfigurationLoaderXml;
 import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
 import com.orientechnologies.orient.server.config.OServerUserConfiguration;
 import com.orientechnologies.orient.server.network.OServerNetworkListener;

 import java.io.IOException;
 import java.util.Arrays;

/**
  * Sends a shutdown command to the server.
  *
  * @author Luca Garulli (l.garulli--at--orientechnologies.com)
  *
  */
 public class OServerShutdownMain {
   public String                           networkAddress;
   public int[]                            networkPort;
   public OChannelBinaryAsynchClient       channel;
   protected OServerConfigurationLoaderXml configurationLoader;
   protected OServerConfiguration          configuration;

   private OContextConfiguration           contextConfig;
   private String                          rootPassword;

   public OServerShutdownMain(final String iServerAddress, final String iServerPorts, final String iRootPassword) {
     contextConfig = new OContextConfiguration();

     try {
       if (iRootPassword == null) {
         // LOAD SERVER ROOT'S PASSWORD
         loadConfiguration();
         if (configuration.users != null && configuration.users.length > 0) {
           for (OServerUserConfiguration u : configuration.users) {
             if (u.name.equals(OServerConfiguration.SRV_ROOT_ADMIN)) {
               // FOUND
               rootPassword = u.password;
               break;
             }
           }
         }
       } else
         rootPassword = iRootPassword;

       if (iServerAddress == null) {
         // LOAD SERVER HOST AND PORT FROM FILE
         loadConfiguration();
         for (OServerNetworkListenerConfiguration l : configuration.network.listeners) {
           if (l.protocol.equals("distributed") || l.protocol.equals("binary")) {
             networkAddress = l.ipAddress;
             networkPort = OServerNetworkListener.getPorts(l.portRange);
             break;
           }
         }
       } else {
         networkAddress = iServerAddress;
         networkPort = OServerNetworkListener.getPorts(iServerPorts);
       }

     } catch (IOException e) {
       OLogManager.instance().error(this, "Error on reading server configuration.", OConfigurationException.class);
     }
   }

   private void loadConfiguration() throws IOException {
     if (configurationLoader != null)
       // AREADY LOADED
       return;

     configurationLoader = new OServerConfigurationLoaderXml(OServerConfiguration.class, ODatabaseHelper.getConfigurationFile());
     configuration = configurationLoader.load();
   }

   public void connect(final int iTimeout) throws IOException {
     // TRY TO CONNECT TO THE RIGHT PORT
     for (int port : networkPort)
       try {
         channel = new OChannelBinaryAsynchClientSynch(networkAddress, port, null, contextConfig);
         break;
       } catch (Exception e) {
         OLogManager.instance().error(this, "Error on connecting to %s:%d", e, networkAddress, port);
       }

     if (channel == null)
       throw new ONetworkProtocolException("Cannot connect to server host '" + networkAddress + "', ports: "
           + Arrays.toString(networkPort));

     channel.writeByte(OChannelBinaryProtocol.REQUEST_SHUTDOWN);
     channel.writeInt(-1);
     channel.writeString(OServerConfiguration.SRV_ROOT_ADMIN);
     channel.writeString(rootPassword);
     channel.flush();

     if (channel.readByte() == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {
       channel.readInt();
       channel.readString();
       throw new ONetworkProtocolException(channel.readString());
     }
     channel.readInt();
   }

   public static void main(final String[] iArgs) {
     String serverHost = iArgs.length > 0 ? iArgs[0] : null;
     String serverPorts = iArgs.length > 1 ? iArgs[1] : null;
     String rootPassword = iArgs.length > 2 ? iArgs[2] : null;

     System.out.println("Sending shutdown command to remote OrientDB Server instance...");

     try {
       new OServerShutdownMain(serverHost, serverPorts, rootPassword).connect(5000);
       System.out.println("Shutdown executed correctly");
     } catch (Exception e) {
       System.out.println("Error: " + e.getLocalizedMessage());
     }
     System.out.println();
   }
 }
