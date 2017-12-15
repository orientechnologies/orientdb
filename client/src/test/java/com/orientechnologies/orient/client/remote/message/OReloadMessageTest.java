package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class OReloadMessageTest {

  private OrientDB         orientDB;
  private ODatabaseSession session;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    session = orientDB.open("test", "admin", "admin");
  }

  @After
  public void after() {
    session.close();
    orientDB.close();
  }

  @Test
  public void testWriteReadResponse() throws IOException {
    OStorageConfiguration configuration = ((ODatabaseDocumentInternal) session).getStorage().getConfiguration();
    OReloadResponse37 responseWrite = new OReloadResponse37(configuration);
    MockChannel channel = new MockChannel();
    responseWrite.write(channel, OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION, null);
    channel.close();
    OReloadResponse37 responseRead = new OReloadResponse37();
    responseRead.read(channel, null);

    assertEquals(configuration.getProperties().size(), responseRead.getProperties().size());
    for (int i = 0; i < configuration.getProperties().size(); i++) {
      assertEquals(configuration.getProperties().get(i).name, responseRead.getProperties().get(i).name);
      assertEquals(configuration.getProperties().get(i).value, responseRead.getProperties().get(i).value);
    }
    assertEquals(configuration.getDateFormat(), responseRead.getDateFormat());
    assertEquals(configuration.getDateTimeFormat(), responseRead.getDateTimeFormat());
    assertEquals(configuration.getName(), responseRead.getName());
    assertEquals(configuration.getVersion(), responseRead.getVersion());
    assertEquals(configuration.getDirectory(), responseRead.getDirectory());
    assertEquals(configuration.getSchemaRecordId(), responseRead.getSchemaRecordId().toString());
    assertEquals(configuration.getIndexMgrRecordId(), responseRead.getIndexMgrRecordId().toString());
    assertEquals(configuration.getClusterSelection(), responseRead.getClusterSelection());
    assertEquals(configuration.getConflictStrategy(), responseRead.getConflictStrategy());
    assertEquals(configuration.isValidationEnabled(), responseRead.isValidationEnabled());
    assertEquals(configuration.getLocaleLanguage(), responseRead.getLocaleLanguage());
    assertEquals(configuration.getMinimumClusters(), responseRead.getMinimumClusters());
    assertEquals(configuration.isStrictSql(), responseRead.isStrictSql());
    assertEquals(configuration.getCharset(), responseRead.getCharset());
    assertEquals(configuration.getLocaleCountry(), responseRead.getLocaleCountry());
    assertEquals(configuration.getTimeZone(), responseRead.getTimeZone());
    assertEquals(configuration.getRecordSerializer(), responseRead.getRecordSerializer());
    assertEquals(configuration.getRecordSerializerVersion(), responseRead.getRecordSerializerVersion());
    assertEquals(configuration.getBinaryFormatVersion(), responseRead.getBinaryFormatVersion());

    assertEquals(configuration.getClusters().size(), responseRead.getClusters().size());
    for (int i = 0; i < configuration.getClusters().size(); i++) {
      assertEquals(configuration.getClusters().get(i).getId(), responseRead.getClusters().get(i).getId());
      assertEquals(configuration.getClusters().get(i).getName(), responseRead.getClusters().get(i).getName());
    }

  }

}
