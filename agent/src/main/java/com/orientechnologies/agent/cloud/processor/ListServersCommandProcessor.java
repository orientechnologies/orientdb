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
      stats.setCpuUsage(getDouble(statsDoc, ""));
      stats.setActiveConnections(getLong(statsDoc, ""));
      stats.setCreateOps(getLong(statsDoc, ""));
      stats.setUpdateOps(getLong(statsDoc, ""));
      stats.setScanOps(getLong(statsDoc, ""));
      stats.setDeleteOps(getLong(statsDoc, ""));

      Map statsMap = ((Map) ((Map) statsDoc.getProperty("reatime")).get("statistics"));
      statsMap.get("process.runtime.availableMemory");

      stats.setTotalHeapMemory(getLong(statsDoc.getProperty("reatime"), "statistics", "process.runtime.availableMemory", "total"));
      stats.setUsedHeapMemory(getLong(statsDoc.getProperty("reatime"), "statistics", "process.runtime.availableMemory", "last"));
      stats.setDeleteOps(getLong(statsDoc.getProperty("reatime"), "counters", "process.runtime.availableMemory", "last"));

      //TODO

      result.addInfo(server);
    } else { //distributed
      final ODocument doc = manager.getClusterConfiguration();

      final Collection<ODocument> documents = doc.field("members");

      for (ODocument document : documents) {
        ServerBasicInfo server = new ServerBasicInfo();
        server.setName((String) document.field("name"));
        server.setId((String) document.field("name"));
        result.addInfo(server);
      }
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
