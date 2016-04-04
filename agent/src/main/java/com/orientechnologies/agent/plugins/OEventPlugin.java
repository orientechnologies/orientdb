/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.plugins;

import com.hazelcast.core.Member;
import com.orientechnologies.agent.event.OEvent;
import com.orientechnologies.agent.event.OEventController;
import com.orientechnologies.agent.profiler.OProfilerData;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OProfilerEntry;
import com.orientechnologies.common.profiler.OProfilerListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginConfigurable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Enrico Risa on 23/11/15.
 */
public class OEventPlugin extends OServerPluginAbstract implements OServerPluginConfigurable {

  private String             configFile = "${ORIENTDB_HOME}/config/events.json";

  private ODocument          configuration;
  private OServer            server;
  protected OEventController eventController;

  @Override
  public ODocument getConfig() {
    synchronized (this) {
      return configuration;
    }
  }

  @Override
  public void changeConfig(ODocument document) {

    synchronized (this) {
      ODocument oldConfig = configuration;
      configuration = document;

      try {
        writeConfiguration();
      } catch (IOException e) {
        OException.wrapException(new OConfigurationException("Cannot Write EventConfiguration configuration file '" + configFile
            + "'. Restoring old configuration."), e);
        configuration = oldConfig;
      }

    }
  }

  public void writeConfiguration() throws IOException {

    final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));

    OIOUtils.writeFile(f, configuration.toJSON("prettyPrint"));
  }

  @Override
  public String getName() {
    return "ee-events";
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    super.config(oServer, iParams);

    this.server = oServer;
    configuration = new ODocument();
    configuration.field("events", new ArrayList<ODocument>());

    final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));

    if (f.exists()) {
      // READ THE FILEw
      try {
        final String configurationContent = OIOUtils.readFileAsString(f);
        configuration = new ODocument().fromJSON(configurationContent);
      } catch (IOException e) {
        OException.wrapException(new OConfigurationException("Cannot load Events configuration file '" + configFile
            + "'. Events  Plugin will be disabled"), e);
      }
    } else {
      try {
        f.getParentFile().mkdirs();
        f.createNewFile();
        OIOUtils.writeFile(f, configuration.toJSON("prettyPrint"));

        OLogManager.instance().info(this, "Events plugin: created configuration to file '%s'", f);
      } catch (IOException e) {
        OException.wrapException(new OConfigurationException("Cannot create Events plugin configuration file '" + configFile
            + "'. Events Plugin will be disabled"), e);
      }
    }
    eventController = new OEventController(this);
    eventController.setDaemon(true);
    eventController.start();

  }

  @Override
  public void startup() {

    final ODistributedServerManager distributedManager = server.getDistributedManager();

    Orient.instance().getProfiler().registerListener(new OProfilerListener() {
      @Override
      public void onUpdateCounter(String iName, long counter, long recordingFrom, long recordingTo) {
        // Do noting
      }

      @Override
      public void onUpdateChrono(OProfilerEntry chrono) {
        // Do noting
      }

      @Override
      public void onSnapshotCreated(Object snapshot) {
        eventController.analyzeSnapshot((OProfilerData) snapshot);

      }
    });
    if (distributedManager != null) {
      distributedManager.registerLifecycleListener(new ODistributedLifecycleListener() {
        @Override
        public boolean onNodeJoining(String iNode) {

          return false;
        }

        @Override
        public void onNodeJoined(String iNode) {
          if (isLeader(distributedManager))
            eventController
                .broadcast(OEvent.EVENT_TYPE.LOG_WHEN, new ODocument().field("server", iNode).field("message", "ONLINE"));
        }

        @Override
        public void onNodeLeft(String iNode) {
          if (isLeader(distributedManager))
            eventController.broadcast(OEvent.EVENT_TYPE.LOG_WHEN, new ODocument().field("server", iNode)
                .field("message", "OFFLINE"));
        }

        @Override
        public void onDatabaseChangeStatus(String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus) {

        }
      });
    }
  }

  @Override
  public void shutdown() {

    eventController.interrupt();
  }

  /**
   * Check if the firstNode (the old one) is local member
   * 
   * @param distributedManager
   * @return
   */
  boolean isLeader(ODistributedServerManager distributedManager) {

    OHazelcastPlugin oHazelcastPlugin = (OHazelcastPlugin) distributedManager;

    Member oldestMember = oHazelcastPlugin.getHazelcastInstance().getCluster().getMembers().iterator().next();

    return oldestMember.localMember();

  }
}
