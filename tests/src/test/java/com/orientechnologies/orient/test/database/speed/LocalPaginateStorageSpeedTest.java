package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.core.version.OSimpleVersion;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import org.testng.annotations.Test;

import java.util.Date;

public class LocalPaginateStorageSpeedTest extends OrientMonoThreadTest {
  private ODatabaseDocumentInternal database;
  private ODocument                 record;
  private Date                      date = new Date();
  private byte[]                    content;
  private OStorage                  storage;

  public LocalPaginateStorageSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Test(enabled = false)
  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    LocalPaginateStorageSpeedTest test = new LocalPaginateStorageSpeedTest();
    test.data.go(test);
  }

  @Override
  @Test(enabled = false)
  public void init() {
    OGlobalConfiguration.USE_WAL.setValue(false);

    ODatabaseDocumentTx.setDefaultSerializer(new ORecordSerializerBinary());
    database = new ODatabaseDocumentTx("plocal:target/db/test");
    if (database.exists()) {
      database.open("admin", "admin");
      database.drop();
    }

    database.create();
    OSchema schema = database.getMetadata().getSchema();
    schema.createClass("Account");

    record = database.newInstance();

    database.declareIntent(new OIntentMassiveInsert());
    database.begin(TXTYPE.NOTX);

    storage = database.getStorage();
  }

  @Override
  @Test(enabled = false)
  public void cycle() {
    record.reset();

    record.setClassName("Account");
    record.field("id", data.getCyclesDone());
    record.field("name", "Luca");
    record.field("surname", "Garulli");
    record.field("birthDate", date);
    record.field("salary", 3000f + data.getCyclesDone());

    content = record.toStream();

    storage.createRecord(new ORecordId(), content, new OSimpleVersion(), (byte) 'd', 0, null);
  }

  @Override
  @Test(enabled = false)
  public void deinit() {
    System.out.println(Orient.instance().getProfiler().dump());

    if (database != null)
      database.close();
    super.deinit();
    OGlobalConfiguration.USE_WAL.setValue(true);
  }

}
