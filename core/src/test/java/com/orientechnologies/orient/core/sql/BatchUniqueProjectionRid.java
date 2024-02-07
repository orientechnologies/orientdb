package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertFalse;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import org.junit.Test;

public class BatchUniqueProjectionRid extends BaseMemoryDatabase {

  @Test
  public void testBatchUniqueRid() {
    List<List<ODocument>> res =
        db.command(
                new OCommandScript(
                    "begin;let $a = select \"a\" as a ; let $b = select \"a\" as b; return"
                        + " [$a,$b] "))
            .execute();

    assertFalse(
        res.get(0).get(0).getIdentity().getClusterPosition()
            == res.get(1).get(0).getIdentity().getClusterPosition());

    //    assertEquals(1, res.get(0).get(0).getIdentity().getClusterPosition());
    //    assertEquals(2, res.get(1).get(0).getIdentity().getClusterPosition());
  }
}
