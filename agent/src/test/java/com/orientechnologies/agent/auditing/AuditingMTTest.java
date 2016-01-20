/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.auditing;

import com.orientechnologies.agent.hook.OAuditingHook;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Enrico Risa on 29/12/15.
 */
public class AuditingMTTest extends TestCase {

  OrientGraphFactory     factory;

  private static Integer threads = 4;
  ExecutorService        executorService;

  CountDownLatch         latch;

  @Override
  protected void setUp() throws Exception {
    factory = new OrientGraphFactory("memory:AuditingMTTest");
    factory.getNoTx().createVertexType("User");
    factory.getNoTx().createVertexType("Test");
    executorService = Executors.newFixedThreadPool(threads);
  }

  public void testBaseCfg() throws ExecutionException, InterruptedException {

    runAndAssert(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {

        OrientBaseGraph graph = graph();

        try {
          graph.getRawGraph().registerHook(
              new OAuditingHook("{classes:{" + "'User':{onCreateEnabled:true, onCreateMessage:'Created new user ${field.name}'},"
                  + "'V':{onCreateEnabled:true, onCreateMessage:'Created vertex of class ${field.@class}'},"
                  + "'*':{onCreateEnabled:false}}}"));

          graph.addVertex("class:User", "name", "Jill");

          graph.addVertex(null, "name", "Jill");

          // TEST NO LOG HAS BEEN CREATED
          new ODocument("Test").field("name", "Jill");

          return 2;
        } finally {
          graph.shutdown();
        }
      }
    });

  }

  protected void runAndAssert(Callable<Integer> callable) throws ExecutionException, InterruptedException {

    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    for (int i = 0; i < threads; i++) {
      Future<Integer> integerFuture = executorService.submit(callable);
      futures.add(integerFuture);
    }

    int count = 0;
    for (Future<Integer> future : futures) {
      count += future.get();
    }

    waitForPropagation();

    OrientBaseGraph graph = graph();
    try {
      assertEquals(count, graph.getRawGraph().countClass("AuditingLog"));
    } finally {
      graph.shutdown();
    }

  }

  @Override
  protected void tearDown() throws Exception {
    factory.drop();
  }

  protected OrientBaseGraph graph() {
    return factory.getNoTx();
  }

  protected void waitForPropagation() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
    }
  }

}
