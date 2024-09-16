package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class NonBlockingQueryTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public NonBlockingQueryTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    database.command("create class Foo").close();
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();
    database.command("delete from Foo").close();
  }

  @Test
  public void testClone() {

    ODatabaseDocumentInternal db = database;

    db.begin();
    db.command("insert into Foo (a) values ('bar')").close();
    db.commit();
    ODatabaseDocumentInternal newDb = db.copy();

    List<OResult> result = newDb.query("Select from Foo").stream().toList();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getProperty("a"), "bar");
    newDb.close();
  }

  @Test
  public void testNonBlockingQuery() {
    ODatabaseDocumentInternal db = database;
    final AtomicInteger counter = new AtomicInteger(0); // db.begin();
    for (int i = 0; i < 1000; i++) {
      db.command("insert into Foo (a) values ('bar')").close();
    }
    Future future =
        db.query(
            new OSQLNonBlockingQuery<Object>(
                "select from Foo",
                new OCommandResultListener() {
                  @Override
                  public boolean result(Object iRecord) {
                    counter.incrementAndGet();
                    return true;
                  }

                  @Override
                  public void end() {}

                  @Override
                  public Object getResult() {
                    return null;
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
    database.command("create property Foo.x integer").close();
    database.command("create property Foo.y integer").close();
    database.command("create index Foo_xy_index on Foo (x, y) notunique").close();

    ODatabaseDocumentInternal db = database;
    final AtomicInteger counter = new AtomicInteger(0); // db.begin();
    for (int i = 0; i < 1000; i++) {
      db.command("insert into Foo (a, x, y) values ('bar', ?, ?)", i, 1000 - i).close();
    }
    Future future =
        db.query(
            new OSQLNonBlockingQuery<Object>(
                "select from Foo where x=500 and y=500",
                new OCommandResultListener() {
                  @Override
                  public boolean result(Object iRecord) {
                    counter.incrementAndGet();
                    return true;
                  }

                  @Override
                  public void end() {}

                  @Override
                  public Object getResult() {
                    return null;
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
