package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class NonBlockingQueryTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public NonBlockingQueryTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    database.command(new OCommandSQL("create class Foo")).execute();
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();
    database.command(new OCommandSQL("delete from Foo")).execute();
  }

  @Test
  public void testClone() {

    ODatabaseDocumentTx db = database;

    db.begin();
    db.command(new OCommandSQL("insert into Foo (a) values ('bar')")).execute();
    db.commit();
    ODatabaseDocumentTx newDb = db.copy();

    List<ODocument> result = newDb.query(new OSQLSynchQuery<ODocument>("Select from Foo"));
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).field("a"), "bar");
    newDb.close();
  }

  @Test
  public void testNonBlockingQuery() {
    ODatabaseDocumentTx db = database;
    final AtomicInteger counter = new AtomicInteger(0); // db.begin();
    for (int i = 0; i < 1000; i++) {
      db.command(new OCommandSQL("insert into Foo (a) values ('bar')")).execute();
    }
    Future future = db.query(new OSQLNonBlockingQuery<Object>("select from Foo", new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        counter.incrementAndGet();
        return true;
      }

      @Override
      public void end() {

      }
    }));
    Assert.assertFalse(counter.get() == 1000);
    try {
      future.get();
      Assert.assertEquals(counter.get(), 1000);
    } catch (InterruptedException e) {
      Assert.fail();
      e.printStackTrace();
    } catch (ExecutionException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testNonBlockingQueryWithCompositeIndex() {
    database.command(new OCommandSQL("create property Foo.x integer")).execute();
    database.command(new OCommandSQL("create property Foo.y integer")).execute();
    database.command(new OCommandSQL("create index Foo_xy_index on Foo (x, y) notunique")).execute();

    ODatabaseDocumentTx db = database;
    final AtomicInteger counter = new AtomicInteger(0); // db.begin();
    for (int i = 0; i < 1000; i++) {
      db.command(new OCommandSQL("insert into Foo (a, x, y) values ('bar', ?, ?)")).execute(i, 1000 - i);
    }
    Future future = db.query(new OSQLNonBlockingQuery<Object>("select from Foo where x=500 and y=500", new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        counter.incrementAndGet();
        return true;
      }

      @Override
      public void end() {

      }
    }));
    Assert.assertFalse(counter.get() == 1);
    try {
      future.get();
      Assert.assertEquals(counter.get(), 1);
    } catch (InterruptedException e) {
      Assert.fail();
      e.printStackTrace();
    } catch (ExecutionException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

}
