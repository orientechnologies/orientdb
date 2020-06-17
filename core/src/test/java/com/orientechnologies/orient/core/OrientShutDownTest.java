package com.orientechnologies.orient.core;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.shutdown.OShutdownHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 01/06/16. */
public class OrientShutDownTest {

  private int test = 0;

  @Before
  public void before() {
    Orient.instance().startup();
  }

  @After
  public void after() {
    Orient.instance().startup();
  }

  @Test
  public void testShutdownHandler() {

    Orient.instance()
        .addShutdownHandler(
            new OShutdownHandler() {
              @Override
              public int getPriority() {
                return 0;
              }

              @Override
              public void shutdown() throws Exception {
                test += 1;
              }
            });

    Orient.instance().shutdown();
    assertEquals(1, test);
    Orient.instance().startup();
    Orient.instance().shutdown();
    assertEquals(1, test);
  }
}
