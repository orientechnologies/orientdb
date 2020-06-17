package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 02/12/15. */
public class TestNullLinkInCollection {

  private ODatabaseDocument db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + TestNullLinkInCollection.class.getSimpleName());
    db.create();
    db.getMetadata().getSchema().createClass("Test");
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testLinkListRemovedRecord() {

    ODocument doc = new ODocument("Test");
    List<ORecordId> docs = new ArrayList<ORecordId>();
    docs.add(new ORecordId(10, 20));
    doc.field("items", docs, OType.LINKLIST);
    db.save(doc);
    List<ODocument> res = db.query(new OSQLSynchQuery<Object>("select items from Test"));
    assertNull(((List) res.get(0).field("items")).get(0));
  }

  @Test
  public void testLinkSetRemovedRecord() {
    ODocument doc = new ODocument("Test");
    Set<ORecordId> docs = new HashSet<ORecordId>();
    docs.add(new ORecordId(10, 20));
    doc.field("items", docs, OType.LINKSET);
    db.save(doc);
    List<ODocument> res = db.query(new OSQLSynchQuery<Object>("select items from Test"));
    assertNull(((Set) res.get(0).field("items")).iterator().next());
  }
}
