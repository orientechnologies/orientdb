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
package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.orientechnologies.common.log.OLogManager;

import java.io.IOException;

/**
 * Strategy used by Hazelcast after a merge of two networks.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OHazelcastMergeStrategy implements MapMergePolicy {
  public OHazelcastMergeStrategy() {
    OLogManager.instance().info(this, "Installed OrientDB Hazelcast Merge Strategy");
  }

  @Override
  public Object merge(final String mapName, final EntryView mergingEntry, final EntryView existingEntry) {
    OLogManager.instance().debug(this, "Merge Strategy map=" + mapName + " key=" + mergingEntry.getKey() + ": "
        + mergingEntry.getValue() + "/" + existingEntry.getValue());

    if (existingEntry.getValue() == null) {
      // NOT PRESENT, USE THE NEW VALUE
      OLogManager.instance().debug(this, "Merge Strategy return mergingEntry=" + mergingEntry.getValue());
      return mergingEntry.getValue();
    }

    // if (mergingEntry.getKey().toString().startsWith(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX)) {
    // final String key = mergingEntry.getKey().toString();
    //
    // final String dbNode = key.substring(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX.length());
    // final String nodeName = dbNode.substring(0, dbNode.indexOf("."));
    // final String databaseName = dbNode.substring(dbNode.indexOf(".") + 1);
    // }
    //
    OLogManager.instance().debug(this, "Merge Strategy return existingEntry=" + existingEntry.getValue());

    return existingEntry.getValue();
  }

  @Override
  public void writeData(final ObjectDataOutput out) throws IOException {
  }

  @Override
  public void readData(final ObjectDataInput in) throws IOException {
  }
}