package com.orientechnologies.tinkerpop;

import com.orientechnologies.orient.core.db.ODatabaseType;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.junit.After;
import org.junit.Before;

/** Created by Enrico Risa on 26/01/17. */
public abstract class AbstractRemoteGraphFactoryTest extends AbstractRemoteTest {

  protected OrientGraphFactory factory;

  @Before
  public void setup() throws Exception {
    super.setup();
    factory =
        new OrientGraphFactory(
            "remote:localhost/" + name.getMethodName(), "root", "root", ODatabaseType.PLOCAL);
  }

  @After
  public void teardown() {
    factory.close();
    super.teardown();
  }
}
