package com.orientechnologies.orient.core;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Test;

import java.util.List;

public class TranasactionsTest {
  @Test
  public void txTest() {
    final ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("plocal:./cw_database");
    databaseDocumentTx.open("admin", "admin");

    final List<ODocument> countRid = databaseDocumentTx.query(new OSQLSynchQuery("select count(@rid) from transactions"));
    final List<ODocument> countCount = databaseDocumentTx.query(new OSQLSynchQuery("select count(*) from transactions"));

    System.out.println("Count rid : " + countRid.get(0).field("count"));
    System.out.println("Count count : " + countCount.get(0).field("count"));

    databaseDocumentTx.close();
  }
}
