package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import java.util.Date;
import org.testng.annotations.Test;

public class LocalPaginateStorageSpeedTest extends OrientMonoThreadTest {
  private ODatabaseDocumentInternal database;
  private ODocument record;
  private Date date = new Date();
  private byte[] content;
  private OAbstractPaginatedStorage storage;

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

    storage = (OAbstractPaginatedStorage) database.getStorage();
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

    storage.createRecord(new ORecordId(), content, 0, (byte) 'd', null);
  }

  @Override
  @Test(enabled = false)
  public void deinit() {
    System.out.println(Orient.instance().getProfiler().dump());

    if (database != null) database.close();
    super.deinit();
  }
}
