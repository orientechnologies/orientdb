package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orientdb.cloud.protocol.*;

import java.util.Collection;
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

      ServerStats stats = new ServerStats();

      Map realtime = statsDoc.getProperty("realtime");

      stats.setTotalHeapMemory(getLong(realtime, "statistics", "process.runtime.availableMemory", "total"));
      stats.setUsedHeapMemory(getLong(realtime, "statistics", "process.runtime.availableMemory", "last"));
      stats.setDeleteOps(aggregate((Map) realtime.get("counters"), "db", "deleteRecord"));
      stats.setUpdateOps(aggregate((Map) realtime.get("counters"), "db", "updateRecord"));
      stats.setCreateOps(aggregate((Map) realtime.get("counters"), "db", "createRecord"));
      stats.setScanOps(aggregate((Map) realtime.get("counters"), "db", "readRecord"));

      stats.setCpuUsage(getDouble(statsDoc, "statistics", "process.runtime.cpu", "last"));
      stats.setNumberOfCPUs(getLong(statsDoc, "sizes", "system.config.cpus"));
      stats.setActiveConnections(getLong(statsDoc, "counters", "server.connections.actives"));
      stats.setNetworkRequets(getLong(statsDoc, "chronos", "server.network.requests", "last"));
      stats.setTotalDiskCache(getLong(statsDoc, "statistics", "process.runtime.diskCacheTotal", "last"));
      stats.setTotalDiskCache(getLong(statsDoc, "statistics", "process.runtime.diskCacheUsed", "last"));
      stats.setDiskSize(getLong(statsDoc, "sizes", "system.disk./.totalSpace"));
      stats.setDiskUsed(
          getLong(realtime,  "sizes", "system.disk./.totalSpace") - getLong(realtime,  "sizes", "system.disk./.freeSpace"));

      server.setStats(stats);

      result.addInfo(server);
    } else { //distributed
      final ODocument doc = manager.getClusterConfiguration();

      final Collection<ODocument> documents = doc.field("members");

      for (ODocument document : documents) {
        ServerBasicInfo server = new ServerBasicInfo();
        server.setName((String) document.field("name"));
        server.setId((String) document.field("name"));
        //TODO server stats

        result.addInfo(server);
      }
    }
    return result;
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
