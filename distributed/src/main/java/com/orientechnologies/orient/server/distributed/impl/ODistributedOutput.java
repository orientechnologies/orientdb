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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.orient.console.OTableFormatter;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Formats information about distributed cfg.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedOutput {

  public static String formatServerStatus(final ODistributedServerManager manager, final ODocument distribCfg) {
    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    final Collection<ODocument> members = distribCfg.field("members");

    if (members != null)
      for (ODocument m : members) {
        if (m == null)
          continue;

        final ODocument serverRow = new ODocument();

        final String serverName = m.field("name");

        serverRow.field("Name", serverName + (manager.getLocalNodeName().equals(serverName) ? "*" : ""));
        serverRow.field("Status", m.field("status"));
        serverRow.field("Databases", (String) null);
        serverRow.field("Conns", m.field("connections"));

        final Date date = m.field("startedOn");

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        if (sdf.format(date).equals(sdf.format(new Date())))
          // TODAY, PUT ONLY THE HOUR
          serverRow.field("StartedOn", new SimpleDateFormat("HH:mm:ss").format(date));
        else
          // ANY OTHER DAY, PUT FULL DATE
          serverRow.field("StartedOn", date);

        final Collection<Map> listeners = m.field("listeners");
        if (listeners != null) {
          for (Map l : listeners) {
            final String protocol = (String) l.get("protocol");
            if (protocol.equals("ONetworkProtocolBinary")) {
              serverRow.field("Binary", l.get("listen"));
            } else if (protocol.equals("ONetworkProtocolHttpDb")) {
              serverRow.field("HTTP", l.get("listen"));
            }
          }
        }

        final Long usedMem = m.field("usedMemory");
        if (usedMem != null) {
          final long maxMem = m.field("maxMemory");

          serverRow.field("UsedMemory", String.format("%s/%s (%.2f%%)", OFileUtils.getSizeAsString(usedMem),
              OFileUtils.getSizeAsString(maxMem), ((float) usedMem / (float) maxMem) * 100));
        }
        rows.add(serverRow);

        final Collection<String> databases = m.field("databases");
        if (databases != null) {
          int serverNum = 0;
          for (String dbName : databases) {
            final StringBuilder buffer = new StringBuilder();
            final ODistributedConfiguration dbCfg = manager.getDatabaseConfiguration(dbName);

            buffer.append(dbName);
            buffer.append("=");
            buffer.append(manager.getDatabaseStatus(serverName, dbName));
            buffer.append(" (");
            buffer.append(dbCfg.getServerRole(serverName));
            buffer.append(")");

            if (serverNum++ == 0)
              // ADD THE 1ST DB IT IN THE SERVER ROW
              serverRow.field("Databases", buffer.toString());
            else
              // ADD IN A SEPARATE ROW
              rows.add(new ODocument().field("Databases", buffer.toString()));
          }
        }
      }

    final StringBuilder buffer = new StringBuilder();
    final OTableFormatter table = new OTableFormatter(new OTableFormatter.OTableOutput() {
      @Override
      public void onMessage(final String text, final Object... args) {
        buffer.append(String.format(text, args));
      }
    });
    table.setColumnHidden("#");
    table.writeRecords(rows, -1);
    buffer.append("\n");
    return buffer.toString();
  }

  public static String formatClusterTable(final ODistributedServerManager manager, final String databaseName,
      final ODistributedConfiguration cfg, final int availableNodes) {
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
    final String defaultOwner = "" + cfg.getClusterOwner(ODistributedConfiguration.ALL_WILDCARD);
    final List<String> defaultServers = cfg.getServers(ODistributedConfiguration.ALL_WILDCARD);

    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();
    final Set<String> allServers = new HashSet<String>();

    for (String cluster : cfg.getClusterNames()) {
      final int wQ = cfg.getWriteQuorum(cluster, availableNodes);
      final int rQ = cfg.getReadQuorum(cluster, availableNodes);
      final String owner = cfg.getClusterOwner(cluster);
      final List<String> servers = cfg.getServers(cluster);

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

          allServers.add(server);

          row.field(server, OAnsiCode.format(server.equals(owner) ? "X" : "o"));
          table.setColumnAlignment(server, OTableFormatter.ALIGNMENT.CENTER);
        }
    }

    for (String server : allServers) {
      table.setColumnMetadata(server, "ROLE", cfg.getServerRole(server).toString());
      table.setColumnMetadata(server, "STATUS", manager.getDatabaseStatus(server, databaseName).toString());
    }

    table.writeRecords(rows, -1);

    buffer.append("\n");

    return buffer.toString();
  }

  public static String formatClasses(final ODistributedConfiguration cfg, final ODatabaseDocument db) {
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
