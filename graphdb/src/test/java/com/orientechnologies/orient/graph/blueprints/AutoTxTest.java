package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.core.exception.OTransactionException;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Sergey Sitnikov
 * @since 23/03/16
 */
public class AutoTxTest {

  private static final String DB_URL = "memory:" + AutoTxTest.class.getSimpleName();
  private static OrientGraph graph;

  @BeforeClass
  public static void before() {
    graph = new OrientGraph(DB_URL);
  }

  @AfterClass
  public static void after() {
    graph.drop();
    graph = null;
  }

  @Test(expected = OTransactionException.class)
  public void testManualTxThrowsWhileAutoTxOn() {
    graph.setAutoStartTx(true);
    graph.begin();
  }

  @Test
  public void testManualTxNotThrowsWhileAutoTxOff() {
    graph.setAutoStartTx(false);
    graph.begin();
  }

}
