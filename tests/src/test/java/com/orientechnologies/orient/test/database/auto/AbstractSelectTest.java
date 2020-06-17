package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public abstract class AbstractSelectTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  protected AbstractSelectTest(@Optional String url) {
    super(url);
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseDocumentInternal db, Object... args) {
    final List<ODocument> synchResult = db.query(new OSQLSynchQuery<ODocument>(sql), args);
    final List<ODocument> asynchResult = new ArrayList<ODocument>();
    final AtomicBoolean endWasCalled = new AtomicBoolean();

    db.query(
        new OSQLAsynchQuery<ODocument>(
            sql,
            new OCommandResultListener() {
              @Override
              public boolean result(Object iRecord) {
                asynchResult.add((ODocument) iRecord);
                return true;
              }

              @Override
              public void end() {
                endWasCalled.set(true);
              }

              @Override
              public Object getResult() {
                return null;
              }
            }),
        args);

    Assert.assertTrue(endWasCalled.get());
    Assert.assertTrue(
        ODocumentHelper.compareCollections(db, synchResult, db, asynchResult, null),
        "Synch: " + synchResult.toString() + ", but asynch: " + asynchResult.toString());

    return synchResult;
  }
}
