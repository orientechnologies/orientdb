package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

/** Created by tglman on 09/05/17. */
public class ORemotePushMessagesTest {

  @Test
  public void testDistributedConfig() throws IOException {
    MockChannel channel = new MockChannel();
    List<String> hosts = new ArrayList<>();
    hosts.add("one");
    hosts.add("two");
    OPushDistributedConfigurationRequest request = new OPushDistributedConfigurationRequest(hosts);
    request.write(channel);
    channel.close();

    OPushDistributedConfigurationRequest readRequest = new OPushDistributedConfigurationRequest();
    readRequest.read(channel);
    assertEquals(readRequest.getHosts().size(), 2);
    assertEquals(readRequest.getHosts().get(0), "one");
    assertEquals(readRequest.getHosts().get(1), "two");
  }

  @Test
  public void testSchema() throws IOException {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseSession session = orientDB.open("test", "admin", "admin");
    ODocument schema =
        ((ODatabaseDocumentInternal) session).getSharedContext().getSchema().toStream();
    session.close();
    orientDB.close();
    MockChannel channel = new MockChannel();

    OPushSchemaRequest request = new OPushSchemaRequest(schema);
    request.write(channel);
    channel.close();

    OPushSchemaRequest readRequest = new OPushSchemaRequest();
    readRequest.read(channel);
    assertNotNull(readRequest.getSchema());
  }

  @Test
  public void testIndexManager() throws IOException {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseSession session = orientDB.open("test", "admin", "admin");
    ODocument schema =
        ((ODatabaseDocumentInternal) session).getSharedContext().getIndexManager().toStream();
    session.close();
    orientDB.close();
    MockChannel channel = new MockChannel();

    OPushIndexManagerRequest request = new OPushIndexManagerRequest(schema);
    request.write(channel);
    channel.close();

    OPushIndexManagerRequest readRequest = new OPushIndexManagerRequest();
    readRequest.read(channel);
    assertNotNull(readRequest.getIndexManager());
  }

  @Test
  public void testStorageConfiguration() throws IOException {
    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseSession session = orientDB.open("test", "admin", "admin");
    OStorageConfiguration configuration =
        ((ODatabaseDocumentInternal) session).getStorage().getConfiguration();
    session.close();
    orientDB.close();
    MockChannel channel = new MockChannel();

    OPushStorageConfigurationRequest request = new OPushStorageConfigurationRequest(configuration);
    request.write(channel);
    channel.close();

    OPushStorageConfigurationRequest readRequest = new OPushStorageConfigurationRequest();
    readRequest.read(channel);
    OStorageConfigurationPayload readPayload = readRequest.getPayload();
    OStorageConfigurationPayload payload = request.getPayload();
    assertEquals(readPayload.getName(), payload.getName());
    assertEquals(readPayload.getDateFormat(), payload.getDateFormat());
    assertEquals(readPayload.getDateTimeFormat(), payload.getDateTimeFormat());
    assertEquals(readPayload.getVersion(), payload.getVersion());
    assertEquals(readPayload.getDirectory(), payload.getDirectory());
    for (OStorageEntryConfiguration readProperty : readPayload.getProperties()) {
      boolean found = false;
      for (OStorageEntryConfiguration property : payload.getProperties()) {
        if (readProperty.name.equals(property.name) && readProperty.value.equals(property.value)) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
    assertEquals(readPayload.getSchemaRecordId(), payload.getSchemaRecordId());
    assertEquals(readPayload.getIndexMgrRecordId(), payload.getIndexMgrRecordId());
    assertEquals(readPayload.getClusterSelection(), payload.getClusterSelection());
    assertEquals(readPayload.getConflictStrategy(), payload.getConflictStrategy());
    assertEquals(readPayload.isValidationEnabled(), payload.isValidationEnabled());
    assertEquals(readPayload.getLocaleLanguage(), payload.getLocaleLanguage());
    assertEquals(readPayload.getMinimumClusters(), payload.getMinimumClusters());
    assertEquals(readPayload.isStrictSql(), payload.isStrictSql());
    assertEquals(readPayload.getCharset(), payload.getCharset());
    assertEquals(readPayload.getTimeZone(), payload.getTimeZone());
    assertEquals(readPayload.getLocaleCountry(), payload.getLocaleCountry());
    assertEquals(readPayload.getRecordSerializer(), payload.getRecordSerializer());
    assertEquals(readPayload.getRecordSerializerVersion(), payload.getRecordSerializerVersion());
    assertEquals(readPayload.getBinaryFormatVersion(), payload.getBinaryFormatVersion());
    for (OStorageClusterConfiguration readCluster : readPayload.getClusters()) {
      boolean found = false;
      for (OStorageClusterConfiguration cluster : payload.getClusters()) {
        if (readCluster.getName().equals(cluster.getName())
            && readCluster.getId() == cluster.getId()) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }

  @Test
  public void testSubscribeRequest() throws IOException {
    MockChannel channel = new MockChannel();

    OSubscribeRequest request =
        new OSubscribeRequest(new OSubscribeLiveQueryRequest("10", new HashMap<>()));
    request.write(channel, null);
    channel.close();

    OSubscribeRequest requestRead = new OSubscribeRequest();
    requestRead.read(channel, 1, ORecordSerializerNetworkV37.INSTANCE);

    assertEquals(request.getPushMessage(), requestRead.getPushMessage());
    assertTrue(requestRead.getPushRequest() instanceof OSubscribeLiveQueryRequest);
  }

  @Test
  public void testSubscribeResponse() throws IOException {
    MockChannel channel = new MockChannel();

    OSubscribeResponse response = new OSubscribeResponse(new OSubscribeLiveQueryResponse(10));
    response.write(channel, 1, ORecordSerializerNetworkV37.INSTANCE);
    channel.close();

    OSubscribeResponse responseRead = new OSubscribeResponse(new OSubscribeLiveQueryResponse());
    responseRead.read(channel, null);

    assertTrue(responseRead.getResponse() instanceof OSubscribeLiveQueryResponse);
    assertEquals(((OSubscribeLiveQueryResponse) responseRead.getResponse()).getMonitorId(), 10);
  }

  @Test
  public void testUnsubscribeRequest() throws IOException {
    MockChannel channel = new MockChannel();
    OUnsubscribeRequest request = new OUnsubscribeRequest(new OUnsubscribeLiveQueryRequest(10));
    request.write(channel, null);
    channel.close();
    OUnsubscribeRequest readRequest = new OUnsubscribeRequest();
    readRequest.read(channel, 0, null);
    assertEquals(
        ((OUnsubscribeLiveQueryRequest) readRequest.getUnsubscribeRequest()).getMonitorId(), 10);
  }
}
