package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) */
public class TestTypeGuessingWorkingWithSQLAndMultiValues extends BaseMemoryDatabase {

  @Before
  public void beforeTest() {
    super.beforeTest();

    db.execute(
            "sql",
            "create class Address\n ;"
                + "create property Address.street String\n;"
                + "create property Address.city String\n;"
                + "\n"
                + "create class Client\n;"
                + "create property Client.name String\n;"
                + "create property Client.phones embeddedSet String\n;"
                + "create property Client.addresses embeddedList Address\n;")
        .close();
  }

  @Test
  public void testLinkedValue() {

    try (OResultSet result =
        db.execute(
            "sql",
            "insert into client set name = 'James Bond', phones = ['1234', '34567'], addresses = [{'@class':'Address','city':'Shanghai', 'zip':'3999'}, {'@class':'Address','city':'New York', 'street':'57th Ave'}]\n;"
                + "update client set addresses = addresses || [{'@type':'d','@class':'Address','city':'London', 'zip':'67373'}] return after;")) {

      Assert.assertTrue(result.hasNext());

      OResult doc = result.next();

      Collection<OResult> addresses = ((Collection<OResult>) doc.getProperty("addresses"));
      Assert.assertEquals(addresses.size(), 3);
      for (OResult a : addresses) Assert.assertTrue(a.getProperty("@class").equals("Address"));
    }

    try (OResultSet result =
        db.command(
            "update client set addresses = addresses || [{'city':'London', 'zip':'67373'}] return after")) {
      Assert.assertTrue(result.hasNext());

      OResult doc = result.next();

      Collection<OResult> addresses = ((Collection<OResult>) doc.getProperty("addresses"));
      Assert.assertEquals(addresses.size(), 4);

      for (OResult a : addresses) Assert.assertTrue(a.getProperty("@class").equals("Address"));
    }
  }
}
