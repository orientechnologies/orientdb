package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import org.junit.Test;

/**
 * Created by SDIPro on 30/09/16.
 */
public class OrientGraphFactorySecurityTest {

  @Test
  public void shouldBypassSecurity() {
    final String url = "memory:" + OrientGraphFactorySecurityTest.class.getSimpleName();

    OrientGraph graph = new OrientGraph(url);
    graph.shutdown();

    OrientGraphFactory gf = new OrientGraphFactory(url, "admin", "invalid").setupPool(5, 100);
    gf.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityNull.class);

    graph = gf.getTx();
    graph.shutdown();

    gf.close();
  }
}
