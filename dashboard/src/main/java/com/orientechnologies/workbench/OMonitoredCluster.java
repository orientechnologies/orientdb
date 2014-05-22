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

  private ODocument         clusterConfig;
  private OWorkbenchPlugin  handler;
  private HazelcastInstance hazelcast;

  OMonitoredCluster(final OWorkbenchPlugin iHandler, final ODocument cluster) {
    this.handler = iHandler;
    this.clusterConfig = cluster;
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
      Map<String, Object> multicast = clusterConfig.field("multicast");
      Config cfg = new Config();
      GroupConfig groupConfig = cfg.getGroupConfig();
      groupConfig.setName(cName);
      groupConfig.setPassword(cPasswd);
      NetworkConfig network = cfg.getNetworkConfig();
      network.setPort(port);
      network.setPortAutoIncrement(portIncrement);
      JoinConfig join = network.getJoin();
      join.getTcpIpConfig().setEnabled(false);
      join.getAwsConfig().setEnabled(false);
      join.getMulticastConfig().setEnabled((Boolean) multicast.get("enabled")).setMulticastGroup((String) multicast.get("group"))
          .setMulticastPort((Integer) multicast.get("port"));
      hazelcast = Hazelcast.newHazelcastInstance(cfg);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void getMemberInfo() {

    // update policy
    for (Member member : getMembers()) {

      IMap<String,Object> maps = getConfigurationMap();
      ODocument doc = (ODocument) getConfigValue("node." + member.getUuid());
      if (doc != null) {
        String nodeName = (String) doc.field("name");
        String iPropertyValue = getAndRemovePwd(nodeName);
        registerNewServer(doc, iPropertyValue);
      }
    }
  }

  private String getAndRemovePwd(String nodeName) {
    String name = "ee." + nodeName;
    String iPropertyValue = (String) getConfigValue(name);
    //removeConfigValue(name);
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
        server.field("password", iPropertyValue);
        server.save();
      }
    }
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
            final String expectedKey = "node." + membershipEvent.getMember().getUuid();
            if (expectedKey.equals(stringObjectEntryEvent.getKey())) {

              ODocument doc = (ODocument) stringObjectEntryEvent.getValue();
              if (doc != null) {
                String nodeName = (String) doc.field("name");
                String pwdString = getAndRemovePwd(nodeName);
                registerNewServer(doc, pwdString);
              }

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

    IMap map = getConfigurationMap();
    return null;
  }
}
