package com.orientechnologies.orient.core.sql.fetch;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class DepthFetchPlanTest {

  private final class CountFetchListener extends ORemoteFetchListener {
    public int count;

    @Override
    protected void sendRecord(ORecord iLinked) {
      count++;
    }
  }

  @Test
  public void testFetchPlanDepth() {
    ODatabaseDocument database = new ODatabaseDocumentTx("memory:" + DepthFetchPlanTest.class.getSimpleName());
    database.create();
    database.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    ODocument doc1 = new ODocument("Test");
    ODocument doc2 = new ODocument("Test");
    doc.field("name", "name");
    database.save(doc);

    doc1.field("name", "name1");
    doc1.field("ref", doc);
    database.save(doc1);
    doc2.field("name", "name2");
    doc2.field("ref", doc1);
    database.save(doc2);
    OFetchContext context = new ORemoteFetchContext();
    CountFetchListener listener = new CountFetchListener();
    OFetchHelper.fetch(doc2, doc2, OFetchHelper.buildFetchPlan("ref:1 *:-2"), listener, context, "");
    assertEquals(1, listener.count);
    database.drop();
  }
}
