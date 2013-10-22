package com.orientechnologies.orient.test.database.speed;

import java.util.Date;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/16/13
 */
public class NotUniqueIndexSpeedTest extends OrientMonoThreadTest {
  private ODatabaseDocumentTx database;
  private int                 counter;
  private Date                date;

  public NotUniqueIndexSpeedTest() throws Exception {
    super(50000);
    date = new Date();
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    super.init();

    database = new ODatabaseDocumentTx("plocal:notUniqueIndexSpeedTest");
    if (database.exists()) {
      database.open("admin", "admin");
      database.drop();
    }

    database.create();

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = schema.createClass("test");
    testClass.createProperty("indexdate", OType.DATE);
    testClass.createIndex("indexdate_index", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "indexdate");
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    String fVal = counter + "123456790qwertyASD";
    counter++;

    database.command(
        new OCommandSQL("insert into test (x,    y,    z,    j,    k ,   l,    m,    indexdate), values (?, ?, ?, ?, ?, ?, ?, ?)"))
        .execute(fVal, fVal, fVal, fVal, fVal, fVal, fVal, date);

  }
}
