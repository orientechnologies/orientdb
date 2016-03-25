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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.orient.console.OTableFormatter;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Formats information about distributed cfg.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedOutput {

  public static String formatClusterTable(final ODistributedConfiguration cfg, final int availableNodes) {
    final StringBuilder buffer = new StringBuilder();

    buffer.append("\n\nLEGEND: X = Owner, o = Copy");

    final OTableFormatter table = new OTableFormatter(new OTableFormatter.OTableOutput() {
      @Override
      public void onMessage(final String text, final Object... args) {
        buffer.append(String.format(text, args));
      }
    });

    table.setColumnSorting("CLUSTER", true);
    table.setColumnHidden("#");

    table.setColumnAlignment("writeQuorum", OTableFormatter.ALIGNMENT.CENTER);
    table.setColumnAlignment("readQuorum", OTableFormatter.ALIGNMENT.CENTER);

    // READ DEFAULT CFG (CLUSTER=*)
    final int defaultWQ = cfg.getWriteQuorum(ODistributedConfiguration.ALL_WILDCARD, availableNodes);
    final int defaultRQ = cfg.getReadQuorum(ODistributedConfiguration.ALL_WILDCARD, availableNodes);
    final String defaultOwner = "" + cfg.getOwnerOfCluster(ODistributedConfiguration.ALL_WILDCARD);
    final List<String> defaultServers = cfg.getOriginalServers(ODistributedConfiguration.ALL_WILDCARD);

    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    for (String cluster : cfg.getClusterNames()) {
      final int wQ = cfg.getWriteQuorum(cluster, availableNodes);
      final int rQ = cfg.getReadQuorum(cluster, availableNodes);
      final String owner = cfg.getOwnerOfCluster(cluster);
      final List<String> servers = cfg.getOriginalServers(cluster);

      if (!cluster.equals(ODistributedConfiguration.ALL_WILDCARD) && defaultWQ == wQ && defaultRQ == rQ
          && defaultOwner.equals(owner) && defaultServers.size() == servers.size() && defaultServers.containsAll(servers))
        // SAME CFG AS THE DEFAULT: DON'T DISPLAY IT
        continue;

      final ODocument row = new ODocument();
      rows.add(row);

      row.field("CLUSTER", cluster);

      row.field("writeQuorum", wQ);
      row.field("readQuorum", rQ);

      if (servers != null)
        for (String server : servers) {
          if (server.equalsIgnoreCase("<NEW_NODE>"))
            continue;

          row.field(server, OAnsiCode.format(server.equals(owner) ? "X" : "o"));
          table.setColumnAlignment(server, OTableFormatter.ALIGNMENT.CENTER);
        }
    }
    table.writeRecords(rows, -1);

    buffer.append("\n");

    return buffer.toString();
  }

  public static String formatClasses(final ODistributedConfiguration cfg, final ODatabaseDocumentTx db) {
    final StringBuilder buffer = new StringBuilder();

    final OTableFormatter table = new OTableFormatter(new OTableFormatter.OTableOutput() {
      @Override
      public void onMessage(final String text, final Object... args) {
        buffer.append(String.format(text, args));
      }
    });

    final Set<String> allServers = cfg.getAllConfiguredServers();

    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    for (OClass cls : db.getMetadata().getSchema().getClasses()) {
      final ODocument row = new ODocument();
      rows.add(row);

      row.field("CLASS", cls.getName());

      final StringBuilder serverBuffer = new StringBuilder();

      for (String server : allServers) {
        final Set<String> clustersOnServer = cfg.getClustersOnServer(server);

        for (String clusterName : clustersOnServer) {
          if (serverBuffer.length() > 0)
            serverBuffer.append(',');

          serverBuffer.append(clusterName);
          serverBuffer.append('(');
          serverBuffer.append(db.getClusterIdByName(clusterName));
          serverBuffer.append(')');
        }
        row.field(server, serverBuffer.toString());
      }
    }
    table.writeRecords(rows, -1);

    return buffer.toString();
  }
}
