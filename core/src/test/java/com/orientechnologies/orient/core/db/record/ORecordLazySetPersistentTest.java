package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class ORecordLazySetPersistentTest extends BaseMemoryDatabase {

  @Test
  public void test1() {
    ORID orid1;
    ORID orid2;

    db.activateOnCurrentThread();
    db.begin();
    {
      ODocument doc1 = new ODocument();
      doc1.field("linkset", new HashSet<ODocument>());
      Set<ODocument> linkset = doc1.field("linkset");
      ODocument doc2 = new ODocument();
      orid2 = doc2.save(db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
      linkset.add(doc2);
      orid1 = doc1.save(db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
      assertNotNull(orid1);
    }
    db.commit();

    db.begin();
    {
      ODocument doc1 = db.load(orid1);
      assertNotNull(doc1);
      Set<ODocument> linkset = doc1.field("linkset");
      assertNotNull(linkset);
      assertEquals(1, linkset.size());

      ODocument doc2 = db.load(orid2);
      assertNotNull(doc2);

      assertEquals(orid2, linkset.iterator().next().getIdentity());
      assertEquals(orid2, doc2.getIdentity());

      linkset.remove(doc2);
      assertEquals(0, linkset.size()); // AssertionError: expected:<0> but was:<1>
    }
    db.commit();
  }

  @Test
  public void test2() {
    ORID orid1;
    ORID orid2;

    db.activateOnCurrentThread();
    db.begin();
    {
      ODocument doc1 = new ODocument();
      doc1.field("linkset", new HashSet<OIdentifiable>());
      Set<OIdentifiable> linkset = doc1.field("linkset");
      ODocument doc2 = new ODocument();
      orid2 = doc2.save(db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
      linkset.add(doc2);
      orid1 = doc1.save(db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
      assertNotNull(orid1);
    }
    db.commit();

    db.begin();
    {
      ODocument doc1 = db.load(orid1);
      assertNotNull(doc1);
      Set<OIdentifiable> linkset = doc1.field("linkset");

      assertNotNull(linkset);
      assertEquals(1, linkset.size());

      ODocument doc2 = db.load(orid2);
      assertNotNull(doc2);

      assertEquals(orid2, linkset.iterator().next().getIdentity());
      assertEquals(orid2, doc2.getIdentity());

      linkset.remove(doc2);
      assertEquals(0, linkset.size()); // AssertionError: expected:<0> but was:<1>
    }
    db.commit();
  }
}
