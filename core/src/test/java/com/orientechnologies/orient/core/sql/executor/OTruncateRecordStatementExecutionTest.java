package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OTruncateRecordStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void truncateRecord() {
    if (!db.getMetadata().getSchema().existsClass("truncateRecord"))
      db.command("create class truncateRecord");

    db.command("insert into truncateRecord (sex, salary) values ('female', 2100)");

    final Long total = db.countClass("truncateRecord");

    final OResultSet resultset =
        db.query("select from truncateRecord where sex = 'female' and salary = 2100");

    OResultSet records =
        db.command("truncate record [" + resultset.next().getElement().get().getIdentity() + "]");

    resultset.close();

    int truncatedRecords = toList(records).size();
    Assert.assertEquals(truncatedRecords, 1);

    OClass cls = db.getMetadata().getSchema().getClass("truncateRecord");
    Set<OIndex> indexes = cls.getIndexes();

    for (OIndex index : indexes) {
      index.rebuild();
    }

    Assert.assertEquals(db.countClass("truncateRecord"), total - truncatedRecords);
  }

  @Test
  public void truncateNonExistingRecord() {
    if (!db.getMetadata().getSchema().existsClass("truncateNonExistingRecord"))
      db.command("create class truncateNonExistingRecord");

    OResultSet records =
        db.command(
            "truncate record [ #"
                + db.getClusterIdByName("truncateNonExistingRecord")
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
