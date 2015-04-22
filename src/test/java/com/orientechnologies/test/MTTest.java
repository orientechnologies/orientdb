/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.test;

import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Enrico Risa on 18/12/14.
 */
@Test
public class MTTest {

  public static final int    OP_NUMBER     = 10000;
  public static final int    THREAD_NUMBER = 10;
  private OrientGraphFactory graphFactory;

  public void testMulti() {

    ExecutorService service = Executors.newFixedThreadPool(10);

    List<Callable<Object>> callables = new ArrayList<Callable<Object>>();
    for (int i = 0; i < THREAD_NUMBER; i++) {
      callables.add(new InsertExecutor(graphFactory, i));
    }
    try {
      service.invokeAll(callables);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    OrientBaseGraph graph = graphFactory.getNoTx();

    Assert.assertEquals(graph.countVertices("TestVertex"), THREAD_NUMBER * OP_NUMBER);
  }

  public class InsertExecutor implements Callable<Object> {
    private OrientGraphFactory graphFactory;
    private int                threadNumber;

    public InsertExecutor(OrientGraphFactory graphFactory, int threadNumber) {

      this.graphFactory = graphFactory;

      this.threadNumber = threadNumber;
    }

    @Override
    public Object call() throws Exception {

      OrientBaseGraph graph = graphFactory.getNoTx();

      for (int i = 0; i < OP_NUMBER; i++) {
        graph.addVertex("class:TestVertex");
      }

      graph.shutdown();
      System.out.println("Thread [" + threadNumber + "] terminated.");
      return null;
    }
  }

  @BeforeClass
  public void init() {
    graphFactory = new OrientGraphFactory("plocal:testMt", "admin", "admin");
    OrientBaseGraph graph = graphFactory.getNoTx();
    graph.createVertexType("TestVertex");
    graph.shutdown();
  }

  @AfterClass
  public void deinit() {
    graphFactory.drop();
  }
}
