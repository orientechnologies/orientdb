package com.orientechnologies.orient.server.query;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class RemoteGraphLiveQueryTest extends BaseServerMemoryDatabase {

  public void beforeTest() {
    super.beforeTest();
    db.createClassIfNotExist("FirstV", "V");
    db.createClassIfNotExist("SecondV", "V");
    db.createClassIfNotExist("TestEdge", "E");
  }

  @Test
  public void testLiveQuery() throws InterruptedException {

    db.command("create vertex FirstV set id = '1'").close();
    db.command("create vertex SecondV set id = '2'").close();
    try (OResultSet resultSet =
        db.command("create edge TestEdge  from (select from FirstV) to (select from SecondV)")) {
      OResult result = resultSet.stream().iterator().next();

      Assert.assertEquals(true, result.isEdge());
    }

    AtomicLong l = new AtomicLong(0);

    db.live(
        "select from SecondV",
        new OLiveQueryResultListener() {

          @Override
          public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
            l.incrementAndGet();
          }

          @Override
          public void onError(ODatabaseDocument database, OException exception) {}

          @Override
          public void onEnd(ODatabaseDocument database) {}

          @Override
          public void onDelete(ODatabaseDocument database, OResult data) {}

          @Override
          public void onCreate(ODatabaseDocument database, OResult data) {}
        },
        new HashMap<String, String>());

    db.command("update SecondV set id = 3");

    Thread.sleep(100);

    Assert.assertEquals(1L, l.get());
  }
}
