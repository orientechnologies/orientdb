package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 3/5/14
 */
@Test
public class PoolTest {

  public void testPool() throws Exception {
    OrientGraphFactory factory = new OrientGraphFactory("plocal:./pooldb").setupPool(1, 10);
    List<OrientGraph> glist = new ArrayList<OrientGraph>();
    for (int j = 0; j < 10; j++) {
      System.out.println("Round = " + j);
      for (int i = 0; i < 10; i++) {
        OrientGraph graph = factory.getTx();
        System.out.println("i = " + i);
        glist.add(graph);
      }
      for (OrientGraph g : glist) {
        g.shutdown();
      }
      glist.clear();
    }
  }
}