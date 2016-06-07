package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.annotation.OId;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 24/05/16.
 */
public class TestBatchRemoteResultSet {

  private OServer server;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException, NotCompliantMBeanException,
      MBeanRegistrationException, InvocationTargetException, NoSuchMethodException, InstantiationException, IOException,
      IllegalAccessException {
    server = new OServer(false);
    server.startup(OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config-single-run.xml"));
    server.activate();
    OServerAdmin admin = new OServerAdmin("remote:localhost:3064");
    admin.connect("root", "root");
    admin.createDatabase(OrientGraphRemoteTest.class.getSimpleName(), "graph", "memory");
    admin.close();
  }

  @Test
  public void runBatchQuery() {

    String batchQuery = "begin; LET t0 = CREATE VERTEX V set mame=\"a\" ;\n LET t1 = CREATE VERTEX V set name=\"b\" ;\n"
        + "LET t2 = CREATE EDGE E FROM $t0 TO $t1 ;\n commit retry 100\n" + "return [$t0,$t1,$t2]";

    OrientGraph graph = new OrientGraph("remote:localhost:3064/" + OrientGraphRemoteTest.class.getSimpleName(), "root", "root");

    Iterable<OIdentifiable> res = graph.getRawGraph().command(new OCommandScript("sql", batchQuery)).execute();
    Iterator iter = res.iterator();
    assertTrue(iter.next() instanceof OIdentifiable);
    assertTrue(iter.next() instanceof OIdentifiable);
    Object edges = iter.next();
    assertTrue(edges instanceof Collection);
    assertTrue(((Collection) edges).iterator().next() instanceof OIdentifiable);

  }

  @After
  public void after() {
    server.shutdown();
    Orient.instance().startup();
  }

}
