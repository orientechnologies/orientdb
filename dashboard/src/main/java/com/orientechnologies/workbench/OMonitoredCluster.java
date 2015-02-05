package com.orientechnologies.workbench;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by enricorisa on 21/05/14.
 */
public class OMonitoredCluster {

  public static final String EE   = "ee.";
  public static final String NODE = "node.";
  private ODocument          clusterConfig;
  private OWorkbenchPlugin   handler;
  private HazelcastInstance  hazelcast;

  OMonitoredCluster(final OWorkbenchPlugin iHandler, final ODocument cluster) {
    this.handler = iHandler;
    this.clusterConfig = cluster;
    init();
  }

  private void init() {
    createHazelcastFromConfig();
    retrievePwd();
    registerMemberChangeListener();
  }

  private void retrievePwd() {
    getMemberInfo();
  }

  private void createHazelcastFromConfig() {
    try {

      String cName = clusterConfig.field("name");
      String cPasswd = clusterConfig.field("password");
      Integer port = clusterConfig.field("port");
      Boolean portIncrement = clusterConfig.field("portIncrement");
      ODocument multicast = clusterConfig.field("multicast");
      ODocument tcp = clusterConfig.field("tcp");
      Config cfg = new Config();
      GroupConfig groupConfig = cfg.getGroupConfig();
      groupConfig.setName(cName);
      groupConfig.setPassword(OL.decrypt(cPasswd));
      NetworkConfig network = cfg.getNetworkConfig();
      network.setPort(port);
      network.setPortAutoIncrement(portIncrement);
      JoinConfig join = network.getJoin();
      join.getTcpIpConfig().setEnabled((Boolean) tcp.field("enabled"));
      Collection<String> members = tcp.field("members");
      for (String m : members) {
        join.getTcpIpConfig().addMember(m);
      }
      join.getAwsConfig().setEnabled(false);
      join.getMulticastConfig().setEnabled((Boolean) multicast.field("enabled"))
          .setMulticastGroup((String) multicast.field("group")).setMulticastPort((Integer) multicast.field("port"));
      hazelcast = Hazelcast.newHazelcastInstance(cfg);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void getMemberInfo() {

    // update policy
    for (Member member : getMembers()) {

      IMap<String, Object> maps = getConfigurationMap();
      ODocument doc = (ODocument) getConfigValue("node." + member.getUuid());
      if (doc != null) {
        String nodeName = (String) doc.field("name");
        String iPropertyValue = getAndRemovePwd(nodeName);
        registerNewServer(doc, iPropertyValue);
      }
    }
  }

  private String getAndRemovePwd(String nodeName) {
    String name = EE + nodeName;
    String iPropertyValue = (String) getConfigValue(name);
    // removeConfigValue(name);
    return iPropertyValue;
  }

  private void registerNewServer(ODocument doc, String iPropertyValue) {
    Collection<Map<String, String>> listeners = doc.field("listeners");
    for (Map<String, String> map : listeners) {
      if ("ONetworkProtocolHttpDb".equals(map.get("protocol"))) {
        OMonitoredServer mServer = handler.getMonitoredServer((String) doc.field("name"));
        ODatabaseRecordThreadLocal.INSTANCE.set(handler.getDb());
        ODocument server = null;
        if (mServer == null) {
          server = new ODocument(OWorkbenchPlugin.CLASS_SERVER);
          server.field("enabled", true);
        } else {
          server = mServer.getConfiguration();
        }
        server.field("name", doc.field("name"));
        server.field("url", map.get("listen"));
        server.field("user", "root");
        server.field("cluster", clusterConfig);
        if (iPropertyValue != null)
          server.field("password", iPropertyValue);
        server.save();
        handler.updateActiveServerList();
      }
    }
  }

  private void changeServerPwd(String nodeName, String pwd) {
    OMonitoredServer mServer = handler.getMonitoredServer(nodeName);
    ODatabaseRecordThreadLocal.INSTANCE.set(handler.getDb());
    ODocument server = null;
    if (mServer == null) {
      server = new ODocument(OWorkbenchPlugin.CLASS_SERVER);
      server.field("enabled", true);
    } else {
      server = mServer.getConfiguration();
    }
    server.field("name", nodeName);
    if (pwd != null)
      server.field("password", pwd);
    server.save();
    handler.updateActiveServerList();
  }

  private Set<Member> getMembers() {
    Set<Member> members = new HashSet<Member>(hazelcast.getCluster().getMembers());
    members.remove(hazelcast.getCluster().getLocalMember());
    return members;
  }

  public Object getConfigValue(String name) {
    return getConfigurationMap().get(name);
  }

  public void removeConfigValue(String name) {
    getConfigurationMap().remove(name);
  }

  public IMap<String, Object> getConfigurationMap() {
    return hazelcast.getMap("orientdb");
  }

  private void registerMemberChangeListener() {

    hazelcast.getCluster().addMembershipListener(new MembershipListener() {
      @Override
      public void memberAdded(final MembershipEvent membershipEvent) {

        final String mapListener = getConfigurationMap().addEntryListener(new EntryListener<String, Object>() {
          @Override
          public void entryAdded(EntryEvent<String, Object> stringObjectEntryEvent) {
            final String key = stringObjectEntryEvent.getKey();
            if (key.startsWith(NODE)) {

              ODocument doc = (ODocument) stringObjectEntryEvent.getValue();
              if (doc != null) {
                String nodeName = (String) doc.field("name");
                String pwdString = getAndRemovePwd(nodeName);
                registerNewServer(doc, pwdString);
              }

            } else if (key.startsWith(EE)) {
              String pwd = (String) stringObjectEntryEvent.getValue();
              String nodeName = key.replace(EE, "");
              changeServerPwd(nodeName, pwd);
            }
          }

          @Override
          public void entryRemoved(EntryEvent<String, Object> stringObjectEntryEvent) {

          }

          @Override
          public void entryUpdated(EntryEvent<String, Object> stringObjectEntryEvent) {

          }

          @Override
          public void entryEvicted(EntryEvent<String, Object> stringObjectEntryEvent) {

          }

          @Override
          public void mapEvicted(MapEvent mapEvent) {

          }

          @Override
          public void mapCleared(MapEvent mapEvent) {

          }
        }, true);

      }

      @Override
      public void memberRemoved(MembershipEvent membershipEvent) {

      }

      @Override
      public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
      }
    });

  }

  public String getName() {
    return clusterConfig.field("name");
  }

  public ODocument getClusterConfig() {

    return clusterConfig;
  }

  public void refreshConfig(ODocument res) {
    clusterConfig = res;
    shutdownDistributed();
    init();
  }

  public void shutdownDistributed() {
    ODatabaseRecordThreadLocal.INSTANCE.set(handler.getDb());
    clusterConfig.reload();
    clusterConfig.field("status", "DISCONNECTED");
    clusterConfig = handler.getDb().save(clusterConfig);
    hazelcast.shutdown();
  }

  public void reInit() {
    clusterConfig.reload();
    clusterConfig = clusterConfig.field("status", "CONNECTED").save();
    init();
  }
}
