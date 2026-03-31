/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Modifiable Distributed configuration. It's created starting from a ODistributedConfiguration
 * object. Every changes increment the field "version".
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OModifiableDistributedConfiguration extends ODistributedConfiguration {
  public OModifiableDistributedConfiguration(final ODocument iConfiguration) {
    super(iConfiguration);
  }

  public OModifiableDistributedConfiguration modify() {
    return this;
  }

  /** Sets the server role between MASTER (default) and REPLICA. */
  public void setServerRole(final String iServerName, final ROLES role) {
    synchronized (configuration) {
      ODocument servers = configuration.field(SERVERS);
      if (servers == null) {
        servers = new ODocument();
        configuration.field(SERVERS, servers, OType.EMBEDDED);
      }

      servers.field(iServerName, role);
      incrementVersion();
    }
  }

  private void incrementVersion() {
    // INCREMENT VERSION
    Integer oldVersion = configuration.field(VERSION);
    if (oldVersion == null) oldVersion = 0;
    configuration.field(VERSION, oldVersion.intValue() + 1);
  }

  public void override(final ODocument newCfg) {
    configuration.fromStream(newCfg.toStream());
    incrementVersion();
  }
}
