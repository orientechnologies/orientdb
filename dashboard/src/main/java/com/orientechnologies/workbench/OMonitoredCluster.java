package com.orientechnologies.workbench;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.orientechnologies.ee.common.OWorkbenchPasswordGet;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
    registerExecutorPwd();
    registerMemberChangeListener();
  }

  private void registerExecutorPwd() {
    final Map<Member, Future<String>> pwds = hazelcast.getExecutorService("default").submitToMembers(new OWorkbenchPasswordGet(),
        getMembers());
    getMemberInfo(pwds);
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
      //hazelcast = Hazelcast.newHazelcastInstance(new FileSystemXmlConfig("config/hazelcast.xml"));
       hazelcast = Hazelcast.newHazelcastInstance(cfg);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void getMemberInfo(Map<Member, Future<String>> members) {

    // update policy
    for (Member member : members.keySet()) {
      ODocument doc = (ODocument) getConfigValue("node." + member.getUuid());
      if (doc != null) {
        String iPropertyValue = null;
        try {
          iPropertyValue = members.get(member).get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
        registerNewServer(doc, iPropertyValue);
      }
    }
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
    return hazelcast.getMap("orientdb").get(name);
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
                Future<String> pwd = hazelcast.getExecutorService("default").submitToMember(new OWorkbenchPasswordGet(),
                    membershipEvent.getMember());
                try {
                  String pwdString = pwd.get();
                  registerNewServer(doc, pwdString);

                } catch (InterruptedException e) {
                  e.printStackTrace();
                } catch (ExecutionException e) {
                  e.printStackTrace();
                }

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

}
