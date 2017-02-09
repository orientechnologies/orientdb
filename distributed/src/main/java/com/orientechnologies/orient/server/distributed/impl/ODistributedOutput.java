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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.orient.console.OTableFormatter;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Formats information about distributed cfg.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
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
        serverRow.field("Status", (Object)m.field("status"));
        serverRow.field("Databases", (String) null);
        serverRow.field("Conns", (Object)m.field("connections"));

        final Date date = m.field("startedOn");

        if (date != null) {
          final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
          if (sdf.format(date).equals(sdf.format(new Date())))
            // TODAY, PUT ONLY THE HOUR
            serverRow.field("StartedOn", new SimpleDateFormat("HH:mm:ss").format(date));
          else
            // ANY OTHER DAY, PUT FULL DATE
            serverRow.field("StartedOn", date);
        }

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

            final ODistributedConfiguration dbCfg = manager.getDatabaseConfiguration(dbName, false);
            if (dbCfg == null)
              continue;

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

  public static String formatLatency(final OHazelcastPlugin manager, final ODocument distribCfg) {
    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    final List<ODocument> members = distribCfg.field("members");

    final StringBuilder buffer = new StringBuilder();
    buffer.append("\nREPLICATION LATENCY AVERAGE (in milliseconds)");
    final OTableFormatter table = new OTableFormatter(new OTableFormatter.OTableOutput() {
      @Override
      public void onMessage(final String text, final Object... args) {
        buffer.append(String.format(text, args));
      }
    });
    table.setColumnHidden("#");

    if (members != null) {
      // BUILD A SORTED SERVER LIST
      final List<String> orderedServers = new ArrayList<String>(members.size());
      for (ODocument fromMember : members) {
        if (fromMember != null) {
          String serverName = fromMember.field("name");
          orderedServers.add(serverName);

          table.setColumnAlignment(formatServerName(manager, serverName), OTableFormatter.ALIGNMENT.RIGHT);
        }
      }
      Collections.sort(orderedServers);

      for (String fromServer : orderedServers) {
        // SEARCH FOR THE MEMBER
        ODocument fromMember = null;
        for (ODocument m : members) {
          if (fromServer.equals(m.field("name"))) {
            fromMember = m;
            break;
          }
        }

        if (fromMember == null)
          // SKIP IT
          continue;

        final ODocument row = new ODocument();
        rows.add(row);

        row.field("Servers", formatServerName(manager, fromServer));

        final ODocument latencies = fromMember.field("latencies");
        if (latencies == null)
          continue;

        for (String toServer : orderedServers) {
          String value = "";
          if (toServer != null && !toServer.equals(fromServer)) {
            final ODocument latency = latencies.field(toServer);
            if (latency != null) {
              value = String.format("%.2f", ((Float) latency.field("average") / 1000000f));
            }
          }
          row.field(formatServerName(manager, toServer), value);
        }
      }
    }

    table.writeRecords(rows, -1);
    buffer.append("\n");
    return buffer.toString();
  }

  public static String formatMessages(final OHazelcastPlugin manager, final ODocument distribCfg) {
    return formatMessageBetweenServers(manager, distribCfg) + formatMessageStats(manager, distribCfg);
  }

  public static String formatMessageBetweenServers(final OHazelcastPlugin manager, final ODocument distribCfg) {
    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    final List<ODocument> members = distribCfg.field("members");

    final StringBuilder buffer = new StringBuilder();
    buffer.append("\nREPLICATION MESSAGE COUNTERS (servers: source on the row and destination on the column)");
    final OTableFormatter table = new OTableFormatter(new OTableFormatter.OTableOutput() {
      @Override
      public void onMessage(final String text, final Object... args) {
        buffer.append(String.format(text, args));
      }
    });
    table.setColumnHidden("#");

    if (members != null) {
      // BUILD A SORTED SERVER LIST
      final List<String> orderedServers = new ArrayList<String>(members.size());
      for (ODocument fromMember : members) {
        if (fromMember != null) {
          String serverName = fromMember.field("name");
          orderedServers.add(serverName);

          table.setColumnAlignment(formatServerName(manager, serverName), OTableFormatter.ALIGNMENT.RIGHT);
        }
      }
      Collections.sort(orderedServers);

      final ODocument rowTotals = new ODocument();

      for (String fromServer : orderedServers) {
        // SEARCH FOR THE MEMBER
        ODocument fromMember = null;
        for (ODocument m : members) {
          if (fromServer.equals(m.field("name"))) {
            fromMember = m;
            break;
          }
        }

        if (fromMember == null)
          // SKIP IT
          continue;

        final ODocument row = new ODocument();
        rows.add(row);

        row.field("Servers", formatServerName(manager, fromServer));

        final ODocument latencies = fromMember.field("latencies");
        if (latencies == null)
          continue;

        long total = 0;
        for (String toServer : orderedServers) {
          final String serverLabel = formatServerName(manager, toServer);

          if (toServer != null && !toServer.equals(fromServer)) {
            final ODocument latency = latencies.field(toServer);
            if (latency != null) {
              final Long entries = (Long) latency.field("entries");
              total += entries;

              row.field(serverLabel, String.format("%,d", entries));

              // AGGREGATE IN TOTALS
              sumTotal(rowTotals, serverLabel, total);
              continue;
            }
          }

          row.field(serverLabel, "");
        }
        row.field("TOTAL", String.format("%,d", total));
        sumTotal(rowTotals, "TOTAL", total);
      }

      // FOOTER WITH THE TOTAL OF ALL THE ROWS
      table.setFooter(rowTotals);

      rowTotals.field("Servers", "TOTAL");
      for (String fromServer : orderedServers) {
        fromServer = formatServerName(manager, fromServer);
        rowTotals.field(fromServer, String.format("%,d", rowTotals.field(fromServer)));
      }
      rowTotals.field("TOTAL", String.format("%,d", rowTotals.field("TOTAL")));

      table.setColumnAlignment("TOTAL", OTableFormatter.ALIGNMENT.RIGHT);
    }

    table.writeRecords(rows, -1);
    buffer.append("\n");
    return buffer.toString();
  }

  public static String formatMessageStats(final OHazelcastPlugin manager, final ODocument distribCfg) {
    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    final List<ODocument> members = distribCfg.field("members");

    final StringBuilder buffer = new StringBuilder();
    buffer.append("\nREPLICATION MESSAGE COORDINATOR STATS");
    final OTableFormatter table = new OTableFormatter(new OTableFormatter.OTableOutput() {
      @Override
      public void onMessage(final String text, final Object... args) {
        buffer.append(String.format(text, args));
      }
    });
    table.setColumnHidden("#");

    if (members != null) {
      // BUILD A SORTED SERVER LIST AND OPERATION NAMES
      final List<String> orderedServers = new ArrayList<String>(members.size());
      final Set<String> operations = new LinkedHashSet<String>();
      for (ODocument fromMember : members) {
        if (fromMember != null) {
          String serverName = fromMember.field("name");
          orderedServers.add(serverName);

          // INSERT ALL THE FOUND OPERATIONS
          final ODocument messages = fromMember.field("messages");
          if (messages != null) {
            for (String opName : messages.fieldNames()) {
              operations.add(opName);
            }
          }
        }
      }

      Collections.sort(orderedServers);

      final ODocument rowTotals = new ODocument();

      for (String server : orderedServers) {
        // SEARCH FOR THE MEMBER
        ODocument member = null;
        for (ODocument m : members) {
          if (server.equals(m.field("name"))) {
            member = m;
            break;
          }
        }

        if (member == null)
          // SKIP IT
          continue;

        final ODocument row = new ODocument();
        rows.add(row);

        row.field("Servers", formatServerName(manager, server));

        final ODocument messages = member.field("messages");
        if (messages == null)
          continue;

        long total = 0;
        for (String opName : operations) {
          final Long counter = messages.field(opName);
          if (counter == null) {
            row.field(opName, "");
            continue;
          }

          total += counter;
          final String value = String.format("%,d", counter);
          row.field(opName, value);

          // AGGREGATE IN TOTALS
          sumTotal(rowTotals, opName, counter);

          table.setColumnAlignment(opName, OTableFormatter.ALIGNMENT.RIGHT);
        }
        row.field("TOTAL", String.format("%,d", total));

        sumTotal(rowTotals, "TOTAL", total);
      }

      // FOOTER WITH THE TOTAL OF ALL THE ROWS
      table.setFooter(rowTotals);

      rowTotals.field("Servers", "TOTAL");
      for (String opName : operations) {
        rowTotals.field(opName, String.format("%,d", rowTotals.field(opName)));
      }
      rowTotals.field("TOTAL", String.format("%,d", rowTotals.field("TOTAL")));
    }

    table.setColumnAlignment("TOTAL", OTableFormatter.ALIGNMENT.RIGHT);

    table.writeRecords(rows, -1);
    buffer.append("\n");
    return buffer.toString();

  }

  protected static void sumTotal(final ODocument rowTotals, final String column, long total) {
    Long totValue = rowTotals.field(column);
    if (totValue == null)
      totValue = 0l;
    rowTotals.field(column, total + totValue);
  }

  /**
   * Create a compact string with all the relevant information.
   *
   * @param manager
   * @param distribCfg
   * @return
   */
  public static String getCompactServerStatus(final ODistributedServerManager manager, final ODocument distribCfg) {
    final StringBuilder buffer = new StringBuilder();

    final Collection<ODocument> members = distribCfg.field("members");

    if (members != null) {
      buffer.append(members.size());
      buffer.append(":[");

      int memberCount = 0;
      for (ODocument m : members) {
        if (m == null)
          continue;

        if (memberCount++ > 0)
          buffer.append(",");

        final String serverName = m.field("name");
        buffer.append(serverName);
        buffer.append((String)m.field("status"));

        final Collection<String> databases = m.field("databases");
        if (databases != null) {
          buffer.append("{");
          int dbCount = 0;
          for (String dbName : databases) {
            final ODistributedConfiguration dbCfg = manager.getDatabaseConfiguration(dbName, false);

            if (dbCfg == null)
              continue;

            if (dbCount++ > 0)
              buffer.append(",");

            buffer.append(dbName);
            buffer.append("=");
            buffer.append(manager.getDatabaseStatus(serverName, dbName));
            buffer.append(" (");
            buffer.append(dbCfg.getServerRole(serverName));
            buffer.append(")");
          }
          buffer.append("}");
        }
      }
      buffer.append("]");
    }

    return buffer.toString();
  }

  public static String formatClusterTable(final ODistributedServerManager manager, final String databaseName,
      final ODistributedConfiguration cfg, final int availableNodes) {
    final StringBuilder buffer = new StringBuilder();

    if (cfg.hasDataCenterConfiguration()) {
      buffer.append("\n\nDATA CENTER CONFIGURATION");
      final OTableFormatter table = new OTableFormatter(new OTableFormatter.OTableOutput() {
        @Override
        public void onMessage(final String text, final Object... args) {
          buffer.append(String.format(text, args));
        }
      });
      table.setColumnSorting("NAME", true);
      table.setColumnHidden("#");
      table.setColumnAlignment("SERVERS", OTableFormatter.ALIGNMENT.LEFT);
      table.setColumnAlignment("writeQuorum", OTableFormatter.ALIGNMENT.CENTER);

      final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

      for (String dcName : cfg.getDataCenters()) {
        final ODocument row = new ODocument();
        rows.add(row);

        final String dcServers = cfg.getDataCenterServers(dcName).toString();

        row.field("NAME", dcName);
        row.field("SERVERS", dcServers.substring(1, dcServers.length() - 1));
        row.field("writeQuorum", cfg.getDataCenterWriteQuorum(dcName));
      }

      table.writeRecords(rows, -1);
    }

    buffer.append("\n\nCLUSTER CONFIGURATION (LEGEND: X = Owner, o = Copy)");

    final OTableFormatter table = new OTableFormatter(new OTableFormatter.OTableOutput() {

      @Override
      public void onMessage(final String text, final Object... args) {
        buffer.append(String.format(text, args));
      }
    });

    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && db.isClosed())
      db = null;

    table.setColumnSorting("CLUSTER", true);
    table.setColumnHidden("#");
    if (db != null)
      table.setColumnAlignment("id", OTableFormatter.ALIGNMENT.RIGHT);
    table.setColumnAlignment("writeQuorum", OTableFormatter.ALIGNMENT.CENTER);
    table.setColumnAlignment("readQuorum", OTableFormatter.ALIGNMENT.CENTER);

    final String localNodeName = manager.getLocalNodeName();

    // READ DEFAULT CFG (CLUSTER=*)
    final String defaultWQ = cfg.isLocalDataCenterWriteQuorum() ? ODistributedConfiguration.QUORUM_LOCAL_DC
        : "" + cfg.getWriteQuorum(ODistributedConfiguration.ALL_WILDCARD, availableNodes, localNodeName);
    final int defaultRQ = cfg.getReadQuorum(ODistributedConfiguration.ALL_WILDCARD, availableNodes, localNodeName);
    final String defaultOwner = "" + cfg.getClusterOwner(ODistributedConfiguration.ALL_WILDCARD);
    final List<String> defaultServers = cfg.getConfiguredServers(ODistributedConfiguration.ALL_WILDCARD);

    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();
    final Set<String> allServers = new HashSet<String>();

    for (String cluster : cfg.getClusterNames()) {
      final String wQ = cfg.isLocalDataCenterWriteQuorum() ? ODistributedConfiguration.QUORUM_LOCAL_DC
          : "" + cfg.getWriteQuorum(cluster, availableNodes, localNodeName);
      final int rQ = cfg.getReadQuorum(cluster, availableNodes, localNodeName);
      final String owner = cfg.getClusterOwner(cluster);
      final List<String> servers = cfg.getConfiguredServers(cluster);

      if (!cluster.equals(ODistributedConfiguration.ALL_WILDCARD) && defaultWQ.equals(wQ) && defaultRQ == rQ
          && defaultOwner.equals(owner) && defaultServers.size() == servers.size() && defaultServers.containsAll(servers))
        // SAME CFG AS THE DEFAULT: DON'T DISPLAY IT
        continue;

      final ODocument row = new ODocument();
      rows.add(row);

      row.field("CLUSTER", cluster);
      if (db != null) {
        final int clId = db.getClusterIdByName(cluster);
        row.field("id", clId > -1 ? clId : "");
      }
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

    final Set<String> registeredServers = cfg.getRegisteredServers();

    for (String server : allServers) {
      table.setColumnMetadata(server, "CFG", registeredServers.contains(server) ? "static" : "dynamic");
      table.setColumnMetadata(server, "ROLE", cfg.getServerRole(server).toString());
      table.setColumnMetadata(server, "STATUS", manager.getDatabaseStatus(server, databaseName).toString());
      if (cfg.hasDataCenterConfiguration())
        table.setColumnMetadata(server, "DC", "DC(" + cfg.getDataCenterOfServer(server) + ")");
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

  protected static String formatServerName(final OHazelcastPlugin manager, final String fromServer) {
    return fromServer + (manager.getLocalNodeName().equals(fromServer) ? "*" : "");
  }
}
