package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OTruncateRecordStatementExecutionTest {
  static ODatabaseDocument database;

  @BeforeClass
  public static void beforeClass() {
    database = new ODatabaseDocumentTx("memory:OTruncateRecordStatementExecutionTest");
    database.create();
  }

  @AfterClass
  public static void afterClass() {
    database.close();
  }

  @Test
  public void truncateRecord() {
    if (!database.getMetadata().getSchema().existsClass("truncateRecord"))
      database.command("create class truncateRecord");

    database.command("insert into truncateRecord (sex, salary) values ('female', 2100)");

    final Long total = database.countClass("truncateRecord");

    final OResultSet resultset =
        database.query("select from truncateRecord where sex = 'female' and salary = 2100");

    OResultSet records =
        database.command(
            "truncate record [" + resultset.next().getElement().get().getIdentity() + "]");

    resultset.close();

    int truncatedRecords = toList(records).size();
    Assert.assertEquals(truncatedRecords, 1);

    OClass cls = database.getMetadata().getSchema().getClass("truncateRecord");
    Set<OIndex> indexes = cls.getIndexes();

    for (OIndex index : indexes) {
      index.rebuild();
    }

    Assert.assertEquals(database.countClass("truncateRecord"), total - truncatedRecords);
  }

  @Test
  public void truncateNonExistingRecord() {
    if (!database.getMetadata().getSchema().existsClass("truncateNonExistingRecord"))
      database.command("create class truncateNonExistingRecord");

    OResultSet records =
        database.command(
            "truncate record [ #"
                + database.getClusterIdByName("truncateNonExistingRecord")
                + ":99999999 ]");

    Assert.assertEquals(toList(records).size(), 0);
  }

  private List<OResult> toList(OResultSet input) {
    List<OResult> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }
}
