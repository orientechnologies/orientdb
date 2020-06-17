package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class LinksetInTransactionTest {

  @Test
  public void test() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:LinksetInTransactionTest");
    db.create();
    try {

      db.createClass("WithLinks").createProperty("links", OType.LINKSET);
      db.createClass("Linked");

      /* A link must already be there */
      OElement withLinks1 = db.newInstance("WithLinks");
      OElement link1 = db.newInstance("Linked");
      link1.save();
      Set set = new HashSet<>();
      set.add(link1);
      withLinks1.setProperty("links", set);
      withLinks1.save();

      /* Only in transaction - without transaction all OK */
      db.begin();

      /* Add a new linked record */
      OElement link2 = db.newInstance("Linked");
      link2.save();
      Set links = withLinks1.getProperty("links");
      links.add(link2);
      withLinks1.save();

      /* Remove all from ORecordLazySet - if only link2 removed all OK */
      links = withLinks1.getProperty("links");
      links.remove(link1);
      links = withLinks1.getProperty("links");
      links.remove(link2);
      withLinks1.save();

      /* All seems OK before commit */
      links = withLinks1.getProperty("links");
      Assert.assertTrue(links.size() == 0);
      links = withLinks1.getProperty("links");
      Assert.assertTrue(links.size() == 0);
      db.commit();

      links = withLinks1.getProperty("links");
      /* Initial record was removed */
      Assert.assertFalse(links.contains(link1));
      /* Fails: why is link2 still in the set? */
      Assert.assertFalse(links.contains(link2));

    } finally {
      db.drop();
    }
  }
}
