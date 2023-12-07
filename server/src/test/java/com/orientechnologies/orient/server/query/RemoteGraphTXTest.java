package com.orientechnologies.orient.server.query;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

/** Created by wolf4ood on 1/03/19. */
public class RemoteGraphTXTest extends BaseServerMemoryDatabase {

  public void beforeTest() {
    super.beforeTest();
    db.createClassIfNotExist("FirstV", "V");
    db.createClassIfNotExist("SecondV", "V");
    db.createClassIfNotExist("TestEdge", "E");
  }

  @Test
  public void itShouldDeleteEdgesInTx() {

    db.command("create vertex FirstV set id = '1'").close();
    db.command("create vertex SecondV set id = '2'").close();
    try (OResultSet resultSet =
        db.command(
            "create edge TestEdge  from ( select from FirstV where id = '1') to ( select from SecondV where id = '2')")) {
      OResult result = resultSet.stream().iterator().next();

      Assert.assertEquals(true, result.isEdge());
    }

    db.begin();

    db
        .command(
            "delete edge TestEdge from (select from FirstV where id = :param1) to (select from SecondV where id = :param2)",
            new HashMap() {
              {
                put("param1", "1");
                put("param2", "2");
              }
            })
        .stream()
        .collect(Collectors.toList());

    db.commit();

    Assert.assertEquals(0, db.query("select from TestEdge").stream().count());

    List<OResult> results =
        db.query("select bothE().size() as count from V").stream().collect(Collectors.toList());

    for (OResult result : results) {
      Assert.assertEquals(0, (int) result.getProperty("count"));
    }
  }
}
