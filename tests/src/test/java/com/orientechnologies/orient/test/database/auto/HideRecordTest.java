package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 3/21/14
 */
@Test
public class HideRecordTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public HideRecordTest(@Optional String url) {
    super(url);
  }

  public void testMassiveRecordsHide() {
    final OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("MassiveRecordsHide")) {
      schema.createClass("MassiveRecordsHide");
    }

    final List<ORID> docs = new ArrayList<ORID>();
    for (int i = 0; i < 100; i++) {
      final ODocument document = new ODocument("MassiveRecordsHide");
      document.field("index", i);
      document.save();

      docs.add(document.getIdentity());
    }

    List<ORID> ridsToRemove = new ArrayList<ORID>();
    for (int i = 0; i < 100; i += 2) {
      database.hide(docs.get(i));

      ridsToRemove.add(docs.get(i));
    }

    for (ORID ridToRemove : ridsToRemove)
      docs.remove(ridToRemove);

    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from MassiveRecordsHide"));
    Assert.assertEquals(result.size(), 50);

    for (ODocument resultDoc : result) {
      Assert.assertTrue(docs.remove(resultDoc.getIdentity()));
    }

    Assert.assertTrue(docs.isEmpty());
  }

  public void testMassiveRecordsHideBySQL() {
    final OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("MassiveRecordsHideBySQL")) {
      schema.createClass("MassiveRecordsHideBySQL");
    }

    final List<ORID> docs = new ArrayList<ORID>();
    for (int i = 0; i < 100; i++) {
      final ODocument document = new ODocument("MassiveRecordsHideBySQL");
      document.field("index", i);
      document.save();

      docs.add(document.getIdentity());
    }

    List<ORID> ridsToRemove = new ArrayList<ORID>();
    for (int i = 0; i < 100; i += 2) {
      final ODocument document = database.load(docs.get(i));
      int result = (Integer) database.command(new OCommandSQL("hide from " + document.getIdentity())).execute();
      Assert.assertEquals(result, 1);

      ridsToRemove.add(docs.get(i));
    }

    for (ORID ridToRemove : ridsToRemove)
      docs.remove(ridToRemove);

    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from MassiveRecordsHideBySQL"));
    Assert.assertEquals(result.size(), 50);

    for (ODocument resultDoc : result) {
      Assert.assertTrue(docs.remove(resultDoc.getIdentity()));
    }

    Assert.assertTrue(docs.isEmpty());
  }
}
