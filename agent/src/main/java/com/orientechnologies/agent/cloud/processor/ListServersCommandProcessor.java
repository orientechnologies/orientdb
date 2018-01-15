package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.http.command.OServerCommandDistributedManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orientdb.cloud.protocol.*;

import java.util.*;
import java.util.stream.Collectors;

public class ListServersCommandProcessor implements CloudCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    ServerList serverList = getClusterConfig(agent.server, agent.server.getDistributedManager());
    response.setPayload(serverList);

    return response;
  }

  public ServerList getClusterConfig(OServer srv, final ODistributedServerManager manager) {
    ServerList result = new ServerList();
    if (manager == null) { //single node
      ServerBasicInfo server = new ServerBasicInfo();

      server.setStartedOn(srv.getStartedOn());
      server.setName("orientdb");
      server.setId("orientdb");
      server.setStatus("ONLINE");

      server.setVersion(OConstants.getRawVersion());
      server.setDistributed(false);
      OProfiler profiler = Orient.instance().getProfiler();
      ODocument statsDoc = new ODocument().fromJSON(profiler.getStatsAsJson());//change this!!!

      Map realtime = statsDoc.getProperty("realtime");

      ServerStats stats = populateStats(server, null, realtime);

      List<String> addresses = srv.getNetworkListeners().stream().map((l) -> l.toString()).collect(Collectors.toList());
      server.setAddresses(addresses);
      server.setStats(stats);

      result.addInfo(server);
    } else { //distributed

      final OServerNetworkListener listener = manager.getServerInstance().getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

      OServerCommandDistributedManager command = (OServerCommandDistributedManager) listener
          .getCommand(OServerCommandDistributedManager.class);

      ODocument clusterStats = command.getClusterConfig(manager);
      Map statsDoc = clusterStats.getProperty("clusterStats");

      List<ODocument> members = clusterStats.field("members");

      if (members != null) {
        List<ServerBasicInfo> collected = members.stream().map(m -> {
          String name = m.field("name");
          Date startedOn = m.field("startedOn");
          String status = m.field("status");
          String version = m.field("version");
          Collection<String> databases = m.field("databases");

          List<Map<String,String>> listeners = m.field("listeners");

          List<String> addresses = new ArrayList<>();
          if(listeners!=null){
            addresses = listeners.stream().map((l) -> l.get("listen")).collect(Collectors.toList());
          }
          ServerBasicInfo server = new ServerBasicInfo();
          server.setDistributed(true);
          server.setName(name);
          server.setId(name);
          server.setVersion(version);
          server.setAddresses(addresses);
          server.setStartedOn(startedOn);
          server.setStatus(status);
          server.setDatabases(databases);
          ODocument nodeStats = (ODocument) statsDoc.get(name);
          if (nodeStats != null) {
            server.setStats(populateStats(server, m, nodeStats.getProperty("realtime")));
          }
          return server;
        }).collect(Collectors.toList());

        result.setInfo(collected);
      }

    }

    return result;
  }

  private ServerStats populateStats(ServerBasicInfo serverBasicInfo, ODocument member, Map realtime) {

    serverBasicInfo.setOsVersion(getString(realtime, "texts", "system.config.os.version"));
    serverBasicInfo.setOsArch(getString(realtime, "texts", "system.config.os.arch"));
    serverBasicInfo.setOsName(getString(realtime, "texts", "system.config.os.name"));
    serverBasicInfo.setJavaVendor(getString(realtime, "texts", "system.config.java.vendor"));
    serverBasicInfo.setJavaVersion(getString(realtime, "texts", "system.config.java.version"));

    ServerStats stats = new ServerStats();
    stats.setTotalHeapMemory(getLong(realtime, "statistics", "process.runtime.availableMemory", "total"));
    stats.setUsedHeapMemory(getLong(realtime, "statistics", "process.runtime.availableMemory", "last"));
    stats.setDeleteOps(aggregate((Map) realtime.get("counters"), "db", "deleteRecord"));
    stats.setUpdateOps(aggregate((Map) realtime.get("counters"), "db", "updateRecord"));
    stats.setCreateOps(aggregate((Map) realtime.get("counters"), "db", "createRecord"));
    stats.setScanOps(aggregate((Map) realtime.get("counters"), "db", "scanRecord"));
    stats.setReadOps(aggregate((Map) realtime.get("counters"), "db", "readRecord"));
    stats.setTxCommitOps(aggregate((Map) realtime.get("counters"), "db", "txCommit"));
    stats.setTxRollbackOps(aggregate((Map) realtime.get("counters"), "db", "txRollback"));
    stats.setConflictOps(aggregate((Map) realtime.get("counters"), "db", "conflictRecord"));
    stats.setDistributedTxRetriesOps(aggregate((Map) realtime.get("counters"), "db", "distributedTxRetries"));

    stats.setCpuUsage(getDouble(realtime, "statistics", "process.runtime.cpu", "last"));
    stats.setNumberOfCPUs(getLong(realtime, "sizes", "system.config.cpus"));
    stats.setActiveConnections(getLong(realtime, "counters", "server.connections.actives"));
    stats.setNetworkRequests(getLong(realtime, "chronos", "server.network.requests", "last"));
    stats.setTotalDiskCache(getLong(realtime, "statistics", "process.runtime.diskCacheTotal", "last"));
    stats.setUsedDiskCache(getLong(realtime, "statistics", "process.runtime.diskCacheUsed", "last"));
    stats.setDiskSize(getLong(realtime, "sizes", "system.disk./.totalSpace"));
    stats.setDiskUsed(
        getLong(realtime, "sizes", "system.disk./.totalSpace") - getLong(realtime, "sizes", "system.disk./.freeSpace"));

    if (member != null) {
      ODocument messages = member.field("messages");
      Map<String, Long> msg = new HashMap<>();

      messages.forEach((e) -> {
        if (e.getValue() instanceof Number) {
          msg.put(e.getKey(), ((Number) e.getValue()).longValue());
        }
      });

      stats.setMessages(msg);
    }

    return stats;
  }

  private Long aggregate(Map<String, Number> counters, String prefix, String suffix) {
    Long result = 0L;
    for (Map.Entry<String, Number> entry : counters.entrySet()) {

      if (prefix != null && !entry.getKey().startsWith(prefix)) {
        continue;
      }
      if (suffix != null && !entry.getKey().endsWith(suffix)) {
        continue;
      }
      result += entry.getValue().longValue();
    }
    return result;
  }

  private Double getDouble(Object statsDoc, String... path) {
    Object value = statsDoc;
    for (String s : path) {
      if (value instanceof Map) {
        value = ((Map) value).get(s);
      }
    }
    if (value instanceof String) {
      value = Double.parseDouble((String) value);
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    return null;
  }

  private String getString(Object statsDoc, String... path) {
    Object value = statsDoc;
    for (String s : path) {
      if (value instanceof Map) {
        value = ((Map) value).get(s);
      }
    }

    if (value instanceof String) {
      return (String) value;
    }
    return null;
  }

  private Long getLong(Object statsDoc, String... path) {
    Object value = statsDoc;
    for (String s : path) {
      if (value instanceof Map) {
        value = ((Map) value).get(s);
      }
    }
    if (value instanceof String) {
      value = Long.parseLong((String) value);
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return 0l;
  }

}
