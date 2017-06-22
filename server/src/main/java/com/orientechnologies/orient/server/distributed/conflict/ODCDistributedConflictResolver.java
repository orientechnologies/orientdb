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

package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Conflict resolver implementation where the majority is selected only in the specified data center. Use this if you have an
 * active-passive configuration.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODCDistributedConflictResolver extends OMajorityDistributedConflictResolver {
  public static final String NAME = "dc";

  @Override
  public OConflictResult onConflict(final String databaseName, final String clusterName, final ORecordId rid,
      final ODistributedServerManager dManager, final Map<Object, List<String>> candidates) {

    if (configuration == null)
      throw new OConfigurationException("DC conflict resolver requires a configuration for the winner");

    final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(databaseName);
    final String winnerDC = configuration.field("winner");

    // FILTER THE RESULT BY REMOVING THE SERVER THAT ARE IN THE CONFIGURED DC
    for (Map.Entry<Object, List<String>> entry : candidates.entrySet()) {
      final List<String> servers = entry.getValue();
      for (Iterator<String> it = servers.iterator(); it.hasNext(); ) {
        final String server = it.next();

        if (!winnerDC.equals(dCfg.getDataCenterOfServer(server)))
          // COLLECT ONLY THE RESULT FROM SERVER IN THE CONFIGURED DC
          it.remove();
      }
    }

    return super.onConflict(databaseName, clusterName, rid, dManager, candidates);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
