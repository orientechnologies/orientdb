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

import com.orientechnologies.orient.console.OTableFormatter;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.ONodeConfig;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Formats information about distributed cfg.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedOutput {

  public static String formatServerStatus(final OrientDBDistributed distr) {
    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    var state = distr.getNodeState();
    var topology = state.getOps().getNetworkTopology();
    final Collection<ONodeId> members = topology.getMembers();

    for (ONodeId m : members) {
      if (m == null) continue;

      final ODocument serverRow = new ODocument();

      final String serverName = m.getNode();

      String serverLabel = serverName;
      if (distr.getNodeName().equals(serverName)) serverLabel += "(*)";

      serverRow.field("Name", serverLabel);
      serverRow.field("Databases", (String) null);
      rows.add(serverRow);
      OCoordinatedDistributedOps ops = state.getOps();
      ODatabasesTopology databaseTopology = ops.getDatabaseTopology();
      final Collection<ODatabaseId> databases = databaseTopology.getDatabases();
      if (databases != null) {
        int serverNum = 0;
        for (ODatabaseId dbId : databases) {
          final StringBuilder buffer = new StringBuilder();

          ODatabaseState databaseStatus = databaseTopology.getState(dbId, m);
          buffer.append(databaseTopology.getDatabaseName(dbId));
          buffer.append("=");
          buffer.append(databaseStatus);
          var role = databaseTopology.getRole(dbId, m);
          if (role != null) {
            buffer.append(" (");
            buffer.append(role);
            buffer.append(")");
          }
          Set<OSyncState> syncs = databaseTopology.getActiveSyncs(dbId);
          for (OSyncState s : syncs) {
            if (s.getDbId().equals(dbId)) {
              if (s.getSender().equals(m)) {
                buffer.append(" (Sending Sync)");
              } else if (s.getReceiver().equals(m)) {
                buffer.append(" (Receiving Sync)");
              }
            }
          }

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
    buffer.append(
        String.format(
            "%s(%s) status, network quorum %d:\n",
            distr.getNodeId(),
            topology.getState().equals(OTopologyState.BOOT) ? "Booting" : "Online",
            topology.getQuorum()));
    final OTableFormatter table =
        new OTableFormatter(
            new OTableFormatter.OTableOutput() {
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

  public static String formatLatency(
      final ODistributedPlugin manager, final OClusterConfiguration distribCfg) {
    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    final Collection<ONodeConfig> members = distribCfg.getMembers();

    final StringBuilder buffer = new StringBuilder();
    buffer.append("\nREPLICATION LATENCY AVERAGE (in milliseconds)");
    final OTableFormatter table =
        new OTableFormatter(
            new OTableFormatter.OTableOutput() {
              @Override
              public void onMessage(final String text, final Object... args) {
                buffer.append(String.format(text, args));
              }
            });
    table.setColumnHidden("#");

    if (members != null) {
      // BUILD A SORTED SERVER LIST
      final List<String> orderedServers = new ArrayList<String>(members.size());
      for (ONodeConfig fromMember : members) {
        if (fromMember != null) {
          String serverName = fromMember.getName();
          orderedServers.add(serverName);

          table.setColumnAlignment(
              formatServerName(manager, serverName), OTableFormatter.ALIGNMENT.RIGHT);
        }
      }
      Collections.sort(orderedServers);

      for (String fromServer : orderedServers) {
        // SEARCH FOR THE MEMBER
        ONodeConfig fromMember = null;
        for (ONodeConfig m : members) {
          if (m != null && fromServer.equals(m.getName())) {
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

        var latencies = fromMember.getLatencies();
        if (latencies == null) continue;
        Collections.sort(latencies, (x, y) -> x.node().getNode().compareTo(y.node().getNode()));
        for (var latency : latencies) {
          String value = String.format("%.2f", (latency.stats().average() / 1000000f));
          row.field(formatServerName(manager, latency.node().getNode()), value);
        }
      }
    }

    table.writeRecords(rows, -1);
    buffer.append("\n");
    return buffer.toString();
  }

  public static String formatMessages(
      final ODistributedPlugin manager, final OClusterConfiguration distribCfg) {
    return formatMessageBetweenServers(manager, distribCfg)
        + formatMessageStats(manager, distribCfg);
  }

  public static String formatMessageBetweenServers(
      final ODistributedPlugin manager, final OClusterConfiguration distribCfg) {
    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    final Collection<ONodeConfig> members = distribCfg.getMembers();

    final StringBuilder buffer = new StringBuilder();
    buffer.append(
        "\n"
            + "REPLICATION MESSAGE COUNTERS (servers: source on the row and destination on the"
            + " column)");
    final OTableFormatter table =
        new OTableFormatter(
            new OTableFormatter.OTableOutput() {
              @Override
              public void onMessage(final String text, final Object... args) {
                buffer.append(String.format(text, args));
              }
            });
    table.setColumnHidden("#");

    if (members != null) {
      // BUILD A SORTED SERVER LIST
      final List<String> orderedServers = new ArrayList<String>(members.size());
      for (ONodeConfig fromMember : members) {
        if (fromMember != null) {
          String serverName = fromMember.getName();
          orderedServers.add(serverName);

          table.setColumnAlignment(
              formatServerName(manager, serverName), OTableFormatter.ALIGNMENT.RIGHT);
        }
      }
      Collections.sort(orderedServers);

      final ODocument rowTotals = new ODocument();

      for (String fromServer : orderedServers) {
        // SEARCH FOR THE MEMBER
        ONodeConfig fromMember = null;
        for (ONodeConfig m : members) {
          if (fromServer.equals(m.getName())) {
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

        long total = 0;
        var latencies = fromMember.getLatencies();
        if (latencies == null) continue;
        Collections.sort(latencies, (x, y) -> x.node().getNode().compareTo(y.node().getNode()));
        for (var latency : latencies) {
          String serverLabel = formatServerName(manager, latency.node().getNode());
          String value = String.format("%.2f", (latency.stats().average() / 1000000f));
          row.field(formatServerName(manager, latency.node().getNode()), value);
          long entries = latency.stats().entries();
          total += entries;

          row.field(serverLabel, String.format("%,d", entries));

          // AGGREGATE IN TOTALS
          sumTotal(rowTotals, serverLabel, total);
        }

        row.field("TOTAL", String.format("%,d", total));
        sumTotal(rowTotals, "TOTAL", total);
      }

      // FOOTER WITH THE TOTAL OF ALL THE ROWS
      table.setFooter(rowTotals);

      rowTotals.field("Servers", "TOTAL");
      for (String fromServer : orderedServers) {
        fromServer = formatServerName(manager, fromServer);
        rowTotals.field(fromServer, String.format("%,d", (Number) rowTotals.field(fromServer)));
      }
      rowTotals.field("TOTAL", String.format("%,d", (Number) rowTotals.field("TOTAL")));

      table.setColumnAlignment("TOTAL", OTableFormatter.ALIGNMENT.RIGHT);
    }

    table.writeRecords(rows, -1);
    buffer.append("\n");
    return buffer.toString();
  }

  public static String formatMessageStats(
      final ODistributedPlugin manager, final OClusterConfiguration distribCfg) {
    final List<OIdentifiable> rows = new ArrayList<OIdentifiable>();

    final Collection<ONodeConfig> members = distribCfg.getMembers();

    final StringBuilder buffer = new StringBuilder();
    buffer.append("\nREPLICATION MESSAGE CURRENT NODE STATS");
    final OTableFormatter table =
        new OTableFormatter(
            new OTableFormatter.OTableOutput() {
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
      for (ONodeConfig fromMember : members) {
        if (fromMember != null) {
          String serverName = fromMember.getName();
          orderedServers.add(serverName);

          // INSERT ALL THE FOUND OPERATIONS
          var messages = fromMember.getMessages();
          if (messages != null) {
            for (var message : messages) {
              operations.add(message.name());
            }
          }
        }
      }

      Collections.sort(orderedServers);

      final ODocument rowTotals = new ODocument();

      for (String server : orderedServers) {
        // SEARCH FOR THE MEMBER
        ONodeConfig member = null;
        for (ONodeConfig m : members) {
          if (server.equals(m.getName())) {
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

        var messages = member.getMessages();
        if (messages == null) continue;

        long total = 0;
        for (String opName : operations) {
          final Optional<Long> counter =
              messages.stream()
                  .filter(x -> x.name().equals(opName))
                  .findFirst()
                  .map(x -> x.messages());
          if (counter.isEmpty()) {
            row.field(opName, "");
            continue;
          }

          total += counter.get();
          final String value = String.format("%,d", counter);
          row.field(opName, value);

          // AGGREGATE IN TOTALS
          sumTotal(rowTotals, opName, counter.get());

          table.setColumnAlignment(opName, OTableFormatter.ALIGNMENT.RIGHT);
        }
        row.field("TOTAL", String.format("%,d", total));

        sumTotal(rowTotals, "TOTAL", total);
      }

      // FOOTER WITH THE TOTAL OF ALL THE ROWS
      table.setFooter(rowTotals);

      rowTotals.field("Servers", "TOTAL");
      for (String opName : operations) {
        rowTotals.field(opName, String.format("%,d", (Number) rowTotals.field(opName)));
      }
      rowTotals.field("TOTAL", String.format("%,d", (Number) rowTotals.field("TOTAL")));
    }

    table.setColumnAlignment("TOTAL", OTableFormatter.ALIGNMENT.RIGHT);

    table.writeRecords(rows, -1);
    buffer.append("\n");
    return buffer.toString();
  }

  protected static void sumTotal(final ODocument rowTotals, final String column, long total) {
    Long totValue = rowTotals.field(column);
    if (totValue == null) totValue = 0l;
    rowTotals.field(column, total + totValue);
  }

  /**
   * Create a compact string with all the relevant information.
   *
   * @param manager
   * @param distribCfg
   * @return
   */
  public static String getCompactServerStatus(
      final ODistributedServerManager manager, final OClusterConfiguration distribCfg) {
    final StringBuilder buffer = new StringBuilder();

    final Collection<ONodeConfig> members = distribCfg.getMembers();

    if (members != null) {
      buffer.append(members.size());
      buffer.append(":[");

      int memberCount = 0;
      for (ONodeConfig m : members) {
        if (m == null) continue;

        if (memberCount++ > 0) buffer.append(",");

        final String serverName = m.getName();
        buffer.append(serverName);
        buffer.append((Object) m.getStatus());

        final Collection<String> databases = m.getDatabases();
        if (databases != null) {
          buffer.append("{");
          int dbCount = 0;
          for (String dbName : databases) {
            OrientDBDistributed ctx =
                (OrientDBDistributed) manager.getServerInstance().getDatabases();
            final ODistributedConfiguration dbCfg = ctx.getExistingDistributedConfiguration(dbName);

            if (dbCfg == null) continue;

            if (dbCount++ > 0) buffer.append(",");

            buffer.append(dbName);
            buffer.append("=");
            buffer.append(ctx.getDatabaseStatus(serverName, dbName));
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

  protected static String formatServerName(
      final ODistributedPlugin manager, final String fromServer) {
    return fromServer + (manager.getLocalNodeName().equals(fromServer) ? "*" : "");
  }

  public static Object formatNewRecordLocks(final ODistributedPlugin manager, final String db) {
    final List<ODocument> rows = getRequestsStatus(manager, db);

    final StringBuilder buffer = new StringBuilder();
    buffer.append("HA RECORD LOCKS FOR DATABASE '" + db + "'");
    final OTableFormatter table =
        new OTableFormatter(
            new OTableFormatter.OTableOutput() {
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

  public static List<ODocument> getRequestsStatus(
      final ODistributedPlugin manager, final String db) {
    final List<ODocument> rows = new ArrayList<ODocument>();

    Map<ODistributedRequestId, ODistributedTxContext> activeTxContexts =
        manager.getDatabase(db).getActiveTxContexts();

    if (activeTxContexts != null) {
      for (Map.Entry<ODistributedRequestId, ODistributedTxContext> entries :
          activeTxContexts.entrySet()) {

        SimpleDateFormat dateFormat =
            new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATETIME_FORMAT);

        ODistributedRequestId key = entries.getKey();
        ODistributedTxContext value = entries.getValue();

        if (value instanceof ONewDistributedTxContextImpl) {

          ONewDistributedTxContextImpl context = (ONewDistributedTxContextImpl) value;

          final ODocument row = new ODocument();

          row.field("requestID", entries.getKey().getMessageId());
          row.field("startedOn", dateFormat.format(new Date(entries.getValue().getStartedOn())));
          row.field("status", context.getStatus().toString());
          row.field(
              "records",
              context.getPromisedRids().stream()
                  .map(r -> r.getKey().toString())
                  .collect(Collectors.toList()));

          rows.add(row);
        }
      }
    }
    return rows;
  }
}
