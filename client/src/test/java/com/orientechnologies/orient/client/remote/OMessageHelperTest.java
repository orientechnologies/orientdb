package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.message.MockChannel;
import com.orientechnologies.orient.client.remote.message.OMessageHelper;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 06/06/2017.
 */
public class OMessageHelperTest {

  @Test
  public void testOIdentifiable() throws IOException {

    OrientDB orientDB = new OrientDB("embedded", OrientDBConfig.defaultConfig());

    orientDB.create("testOIdentifiable", ODatabaseType.MEMORY);

    ODatabaseDocument open = orientDB.open("testOIdentifiable", "admin", "admin");
    int id = open.getClusterIdByName("V");
    try {
      MockChannel channel = new MockChannel();
      ODocument doc = new ODocument();
      ORidBag bags = new ORidBag();
      bags.add(new ORecordId(id, 0));
      doc.field("bag", bags);

      ODocumentInternal.fillClassNameIfNeeded(doc, "Test");
      ORecordInternal.setIdentity(doc, new ORecordId(id, 1));
      ORecordInternal.setVersion(doc, 1);

      OMessageHelper.writeIdentifiable(channel, doc, ORecordSerializerNetworkFactory.INSTANCE.current());
      channel.close();

      ODocument newDoc = (ODocument) OMessageHelper.readIdentifiable(channel, ORecordSerializerNetworkFactory.INSTANCE.current());

      assertThat(newDoc.getClassName()).isEqualTo("Test");
      assertThat((ORidBag) newDoc.field("bag")).hasSize(1);

      ODirtyManager dirtyManager = ORecordInternal.getDirtyManager(newDoc);
      assertThat(dirtyManager.getNewRecords()).isNull();

    } finally {
      open.close();
      orientDB.close();
    }

  }
}
