/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.1-SNAPSHOT (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.1-SNAPSHOT
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.agent.hook;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import junit.framework.TestCase;

import java.util.Date;

/**
 *
 * @author Luca Garulli
 */
public class AuditingTest extends TestCase {
  private OrientGraphNoTx graph;

  @Override
  protected void setUp() {
    graph = new OrientGraphNoTx("memory:AuditingTest");
    graph.createVertexType("User");
  }

  public void testBaseCfg() {
    graph.getRawGraph().registerHook(
        new OAuditingHook("{classes:{" + "'User':{onCreateEnabled:true, onCreateMessage:'Created new user ${field.name}'},"
            + "'V':{onCreateEnabled:true, onCreateMessage:'Created vertex of class ${field.@class}'},"
            + "'*':{onCreateEnabled:false}}}"));

    // TEST CASE OF USER CLASS
    OrientVertex v = graph.addVertex("class:User", "name", "Jill");

    assertEquals(1, graph.getRawGraph().countClass("AuditingLog"));
    ODocument log = getLastLog();

    assertTrue(((Date) log.field("date")).compareTo(new Date()) <= 0);
    assertEquals("admin", ((ODocument) log.field("user")).field("name"));
    assertEquals(ORecordOperation.CREATED, log.field("operation"));
    assertEquals(v.getIdentity(), log.rawField("record"));
    assertEquals("Created new user Jill", log.field("note"));
    assertNull(log.field("changes"));

    // TEST CASE OF V CLASS
    v = graph.addVertex(null, "name", "Jill");

    assertEquals(2, graph.getRawGraph().countClass("AuditingLog"));
    log = getLastLog();

    assertTrue(((Date) log.field("date")).compareTo(new Date()) <= 0);
    assertEquals("admin", ((ODocument) log.field("user")).field("name"));
    assertEquals(ORecordOperation.CREATED, log.field("operation"));
    assertEquals(v.getIdentity(), log.rawField("record"));
    assertEquals("Created vertex of class V", log.field("note"));
    assertNull(log.field("changes"));

    // TEST NO LOG HAS BEEN CREATED
    new ODocument("Test").field("name", "Jill");

    assertEquals(2, graph.getRawGraph().countClass("AuditingLog"));
  }

  public void testPolymorphismEnabled() {
    graph.getRawGraph().registerHook(
        new OAuditingHook("{classes:{" + "'V':{onCreateEnabled:true, onCreateMessage:'Created vertex of class ${field.@class}'},"
            + "'*':{onCreateEnabled:false}}}"));

    // TEST CASE OF USER CLASS
    OrientVertex v = graph.addVertex("class:User", "name", "Jill");

    assertEquals(1, graph.getRawGraph().countClass("AuditingLog"));
    ODocument log = getLastLog();

    assertTrue(((Date) log.field("date")).compareTo(new Date()) <= 0);
    assertEquals("admin", ((ODocument) log.field("user")).field("name"));
    assertEquals(ORecordOperation.CREATED, log.field("operation"));
    assertEquals(v.getIdentity(), log.rawField("record"));
    assertEquals("Created vertex of class User", log.field("note"));
    assertNull(log.field("changes"));
  }

  public void testPolymorphismDisabled() {
    graph.getRawGraph().registerHook(
        new OAuditingHook("{classes:{"
            + "'V':{polymorphic:false, onCreateEnabled:true, onCreateMessage:'Created vertex of class ${field.@class}'},"
            + "'*':{onCreateEnabled:false}}}"));

    // TEST CASE OF USER CLASS
    OrientVertex v = graph.addVertex("class:User", "name", "Jill");

    assertEquals(0, graph.getRawGraph().countClass("AuditingLog"));
  }

  public void testCRUDOperations() {
    graph.getRawGraph().registerHook(
        new OAuditingHook("{classes:{" + "'V':{onCreateEnabled:true, onCreateMessage:'Created vertex of class ${field.@class}',"
            + "onReadEnabled:true, onReadMessage:'Read vertex of class ${field.@class}',"
            + "onUpdateEnabled:true, onUpdateMessage:'Updated vertex of class ${field.@class}',"
            + "onDeleteEnabled:true, onDeleteMessage:'Deleted vertex of class ${field.@class}'}}}"));

    // TEST CREATE
    OrientVertex v = graph.addVertex("class:User", "name", "Jill");

    assertEquals(1, graph.getRawGraph().countClass("AuditingLog"));
    ODocument log = getLastLog();

    assertTrue(((Date) log.field("date")).compareTo(new Date()) <= 0);
    assertEquals("admin", ((ODocument) log.field("user")).field("name"));
    assertEquals(ORecordOperation.CREATED, log.field("operation"));
    assertEquals(v.getIdentity(), log.rawField("record"));
    assertEquals("Created vertex of class User", log.field("note"));
    assertNull(log.field("changes"));

    // TEST UPDATE
    v.setProperty("name", "Jay");

    assertEquals(2, graph.getRawGraph().countClass("AuditingLog"));
    log = getLastLog();

    assertTrue(((Date) log.field("date")).compareTo(new Date()) <= 0);
    assertEquals("admin", ((ODocument) log.field("user")).field("name"));
    assertEquals(ORecordOperation.UPDATED, log.field("operation"));
    assertEquals(v.getIdentity(), log.rawField("record"));
    assertEquals("Updated vertex of class User", log.field("note"));
    assertNull(log.field("changes"));

    // TEST READ
    v.reload();

    assertEquals(3, graph.getRawGraph().countClass("AuditingLog"));
    log = getLastLog();

    assertTrue(((Date) log.field("date")).compareTo(new Date()) <= 0);
    assertEquals("admin", ((ODocument) log.field("user")).field("name"));
    assertEquals(ORecordOperation.LOADED, log.field("operation"));
    assertEquals(v.getIdentity(), log.rawField("record"));
    assertEquals("Read vertex of class User", log.field("note"));
    assertNull(log.field("changes"));

    // TEST DELETE
    v.remove();

    // NOTE: DELETE EXECUTES ALSO A READ
    assertTrue(graph.getRawGraph().countClass("AuditingLog") >= 4);
    log = getLastLog();

    assertTrue(((Date) log.field("date")).compareTo(new Date()) <= 0);
    assertEquals("admin", ((ODocument) log.field("user")).field("name"));
    assertEquals(ORecordOperation.DELETED, log.field("operation"));
    assertEquals(v.getIdentity(), log.rawField("record"));
    assertEquals("Deleted vertex of class User", log.field("note"));
    assertNull(log.field("changes"));
  }

  @Override
  protected void tearDown() {
    graph.drop();
  }

  protected ODocument getLastLog() {
    final ORecordIteratorClass<ODocument> logs = graph.getRawGraph().browseClass("AuditingLog");

    ODocument log = null;

    while (logs.hasNext())
      log = logs.next();

    return log;
  }
}
