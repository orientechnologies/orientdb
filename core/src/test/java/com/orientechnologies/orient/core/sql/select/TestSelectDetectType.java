package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Test;

public class TestSelectDetectType extends BaseMemoryDatabase {

  @Test
  public void testFloatDetection() {

    List<ODocument> res =
        db.query(
            new OSQLSynchQuery<ODocument>("select ty.type() from ( select 1.021484375 as ty)"));
    System.out.println(res.get(0));
    assertEquals(res.get(0).field("ty"), "DOUBLE");
  }
}
