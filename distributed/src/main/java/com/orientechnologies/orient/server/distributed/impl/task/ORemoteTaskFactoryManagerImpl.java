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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.orient.server.distributed.*;

import java.io.IOException;
import java.util.Collection;

/**
 * Factory for remote tasks.
 *
 * @author Luca Garulli
 */
public class ORemoteTaskFactoryManagerImpl implements ORemoteTaskFactoryManager {
  private final ODistributedServerManager dManager;
  private ORemoteTaskFactory[] factories = new ODefaultRemoteTaskFactoryV0[3];

  public ORemoteTaskFactoryManagerImpl(final ODistributedServerManager dManager) {
    this.dManager = dManager;
    factories[0] = new ODefaultRemoteTaskFactoryV0();
    factories[1] = new ODefaultRemoteTaskFactoryV1();
    factories[2] = new ODefaultRemoteTaskFactoryV2();
  }

  @Override
  public ORemoteTaskFactory getFactoryByServerId(final int serverId) {
    final String remoteNodeName = dManager.getNodeNameById(serverId);
    if (remoteNodeName == null)
      throw new IllegalArgumentException("Invalid serverId " + serverId);

    return getFactoryByServerName(remoteNodeName);
  }

  @Override
  public ORemoteTaskFactory getFactoryByServerNames(final Collection<String> serverNames) {
    int minVersion = ORemoteServerController.CURRENT_PROTOCOL_VERSION;
    ORemoteTaskFactory factory = getFactoryByVersion(minVersion);

    for (String server : serverNames) {
      final ORemoteTaskFactory f = getFactoryByServerName(server);
      if (f.getProtocolVersion() < minVersion) {
        factory = f;
        minVersion = f.getProtocolVersion();
      }
    }

    return factory;
  }

  @Override
  public ORemoteTaskFactory getFactoryByServerName(final String serverName) {
    try {
      final ORemoteServerController remoteServer = dManager.getRemoteServer(serverName);

      final ORemoteTaskFactory factory = getFactoryByVersion(remoteServer.getProtocolVersion());
      if (factory == null)
        throw new IllegalArgumentException("Cannot find a factory for remote task for server " + serverName);

      return factory;

    } catch (ODistributedException e) {
      // SERVER NOT AVAILABLE, CONSIDER CURRENT PROTOCOL FOR THE MISSING ONE
      return getFactoryByVersion(ORemoteServerController.CURRENT_PROTOCOL_VERSION);

    } catch (IOException e) {
      throw OException.wrapException(new OIOException("Cannot determine protocol version for server " + serverName), e);
    }
  }

  @Override
  public ORemoteTaskFactory getFactoryByVersion(final int version) {
    if (version < 0 || version >= factories.length)
      throw new IllegalArgumentException("Invalid remote task factory version " + version);

    return factories[version];
  }
}