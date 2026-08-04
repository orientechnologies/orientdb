package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OReloadMessageTest {

  private OrientDB orientDB;
  private ODatabaseSession session;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    session = orientDB.open("test", "admin", "admin");
  }

  @After
  public void after() {
    session.close();
    orientDB.close();
  }

  @Test
  public void testWriteReadResponse() throws IOException {
    OStorageConfiguration configuration =
        ((ODatabaseDocumentInternal) session).getStorage().getConfiguration();
    OReloadResponse37 responseWrite = new OReloadResponse37(configuration);
    MockChannel channel = new MockChannel();
    responseWrite.write(
        channel.getChannelDataOutput(), OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION, null);
    channel.close();
    OReloadResponse37 responseRead = new OReloadResponse37();
    responseRead.read(channel.getChannelDataInput());
    OStorageConfigurationPayload payload = responseRead.getPayload();
    assertEquals(configuration.getProperties().size(), payload.getProperties().size());
    Map<String, String> expectedProps = new HashMap<>();
    for (OStorageEntryConfiguration entry : configuration.getProperties()) {
      expectedProps.put(entry.name, entry.value);
    }
    Map<String, String> actualProps = new HashMap<>();
    for (OStorageEntryConfiguration entry : payload.getProperties()) {
      actualProps.put(entry.name, entry.value);
    }
    assertEquals(expectedProps, actualProps);
    assertEquals(configuration.getDateFormat(), payload.getDateFormat());
    assertEquals(configuration.getDateTimeFormat(), payload.getDateTimeFormat());
    assertEquals(configuration.getName(), payload.getName());
    assertEquals(configuration.getVersion(), payload.getVersion());
    assertEquals(configuration.getDirectory(), payload.getDirectory());
    assertEquals(configuration.getSchemaRecordId(), payload.getSchemaRecordId().toString());
    assertEquals(configuration.getIndexMgrRecordId(), payload.getIndexMgrRecordId().toString());
    assertEquals(configuration.getClusterSelection(), payload.getClusterSelection());
    assertEquals(configuration.getConflictStrategy(), payload.getConflictStrategy());
    assertEquals(configuration.isValidationEnabled(), payload.isValidationEnabled());
    assertEquals(configuration.getLocaleLanguage(), payload.getLocaleLanguage());
    assertEquals(configuration.getMinimumClusters(), payload.getMinimumClusters());
    assertEquals(configuration.isStrictSql(), payload.isStrictSql());
    assertEquals(configuration.getCharset(), payload.getCharset());
    assertEquals(configuration.getLocaleCountry(), payload.getLocaleCountry());
    assertEquals(configuration.getTimeZone(), payload.getTimeZone());
    assertEquals(configuration.getRecordSerializer(), payload.getRecordSerializer());
    assertEquals(configuration.getRecordSerializerVersion(), payload.getRecordSerializerVersion());
    assertEquals(configuration.getBinaryFormatVersion(), payload.getBinaryFormatVersion());

    assertEquals(configuration.getClusters().size(), payload.getClusters().size());
    for (int i = 0; i < configuration.getClusters().size(); i++) {
      assertEquals(
          configuration.getClusters().get(i).getId(), payload.getClusters().get(i).getId());
      assertEquals(
          configuration.getClusters().get(i).getName(), payload.getClusters().get(i).getName());
    }
  }
}
