package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.http.command.OServerCommandDistributedManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orientdb.cloud.protocol.*;

import java.util.Map;

public class ListServersCommandProcessor implements CloudCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    ServerList serverList = getClusterConfig(agent.server.getDistributedManager());
    response.setPayload(serverList);

    return response;
  }

  public ServerList getClusterConfig(final ODistributedServerManager manager) {
    ServerList result = new ServerList();
    if (manager == null) { //single node
      ServerBasicInfo server = new ServerBasicInfo();
      server.setName("orientdb");
      server.setId("orientdb");
      OProfiler profiler = Orient.instance().getProfiler();
      ODocument statsDoc = new ODocument().fromJSON(profiler.getStatsAsJson());//change this!!!

      Map realtime = statsDoc.getProperty("realtime");

      ServerStats stats = populateStats(realtime);

      server.setStats(stats);

      result.addInfo(server);
    } else { //distributed
      final OServerNetworkListener listener = manager.getServerInstance().getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

      OServerCommandDistributedManager command = (OServerCommandDistributedManager) listener
          .getCommand(OServerCommandDistributedManager.class);

      ODocument clusterStats = command.getClusterConfig(manager);
      Map statsDoc = clusterStats.getProperty("clusterStats");
      Map realtime = (Map) statsDoc.get("realtime");

      Iterable<Map.Entry<String, Map>> nodesStats = statsDoc.entrySet();

      for (Map.Entry<String, Map> nodesStat : nodesStats) {
        ServerBasicInfo server = new ServerBasicInfo();
        server.setName(nodesStat.getKey());
        server.setId(nodesStat.getKey());
        ServerStats stats = populateStats(((ODocument)nodesStat.getValue()).getProperty("realtime"));
        server.setStats(stats);
        result.addInfo(server);
      }
    }

    return result;
  }

  private ServerStats populateStats(Map realtime) {
    ServerStats stats = new ServerStats();
    stats.setTotalHeapMemory(getLong(realtime, "statistics", "process.runtime.availableMemory", "total"));
    stats.setUsedHeapMemory(getLong(realtime, "statistics", "process.runtime.availableMemory", "last"));
    stats.setDeleteOps(aggregate((Map) realtime.get("counters"), "db", "deleteRecord"));
    stats.setUpdateOps(aggregate((Map) realtime.get("counters"), "db", "updateRecord"));
    stats.setCreateOps(aggregate((Map) realtime.get("counters"), "db", "createRecord"));
    stats.setScanOps(aggregate((Map) realtime.get("counters"), "db", "readRecord"));

    stats.setCpuUsage(getDouble(realtime, "statistics", "process.runtime.cpu", "last"));
    stats.setNumberOfCPUs(getLong(realtime, "sizes", "system.config.cpus"));
    stats.setActiveConnections(getLong(realtime, "counters", "server.connections.actives"));
    stats.setNetworkRequests(getLong(realtime, "chronos", "server.network.requests", "last"));
    stats.setTotalDiskCache(getLong(realtime, "statistics", "process.runtime.diskCacheTotal", "last"));
    stats.setTotalDiskCache(getLong(realtime, "statistics", "process.runtime.diskCacheUsed", "last"));
    stats.setDiskSize(getLong(realtime, "sizes", "system.disk./.totalSpace"));
    stats.setDiskUsed(
        getLong(realtime, "sizes", "system.disk./.totalSpace") - getLong(realtime, "sizes", "system.disk./.freeSpace"));
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
    return null;
  }

}
