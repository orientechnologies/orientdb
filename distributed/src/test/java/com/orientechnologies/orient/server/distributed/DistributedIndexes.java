package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.impls.orient.*;
import junit.framework.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * @author Luigi Dell'Aquila
 */
public class DistributedIndexes extends AbstractServerClusterTest {
  private final static int SERVERS = 2;

  public String getDatabaseName() {
    return "DistributedIndexesTest";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/server1/databases/" + getDatabaseName());
    db.open("admin", "admin");

    try {

      testIndexUsage(db);
      testIndexAcceptsNulls(db);

    } finally {
      db.close();
    }
    testODistributedRecordLockedExceptionExceptionThrown();
  }

  @Override
  protected void onAfterDatabaseCreation(OrientBaseGraph db) {
    db.command(new OCommandSQL("CREATE CLASS Person extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Person.name STRING")).execute();
    db.command(new OCommandSQL("CREATE INDEX Person.name NOTUNIQUE METADATA { ignoreNullValues: false }")).execute();
  }

  private void testIndexAcceptsNulls(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("CREATE VERTEX Person SET name = 'Tobie'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person SET temp = true")).execute();
  }

  private void testIndexUsage(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("create class DistributedIndexTest")).execute();
    db.command(new OCommandSQL("create property DistributedIndexTest.unique STRING")).execute();
    db.command(new OCommandSQL("create property DistributedIndexTest.notunique STRING")).execute();
    db.command(new OCommandSQL("create property DistributedIndexTest.dictionary STRING")).execute();
    db.command(new OCommandSQL("create property DistributedIndexTest.unique_hash STRING")).execute();
    db.command(new OCommandSQL("create property DistributedIndexTest.notunique_hash STRING")).execute();

    try {
      db.command(new OCommandSQL("CREATE INDEX index_unique         ON DistributedIndexTest (unique) UNIQUE")).execute();
      db.command(new OCommandSQL("CREATE INDEX index_notunique      ON DistributedIndexTest (notunique) NOTUNIQUE")).execute();
      db.command(new OCommandSQL("CREATE INDEX index_dictionary     ON DistributedIndexTest (dictionary) DICTIONARY")).execute();
      db.command(new OCommandSQL("CREATE INDEX index_unique_hash    ON DistributedIndexTest (unique_hash) UNIQUE_HASH_INDEX"))
          .execute();
      db.command(new OCommandSQL("CREATE INDEX index_notunique_hash ON DistributedIndexTest (notunique_hash) NOTUNIQUE_HASH_INDEX"))
          .execute();

      final ODocument test1 = new ODocument("DistributedIndexTest");
      test1.field("unique", "test1");
      test1.field("notunique", "test1");
      test1.field("dictionary", "test1");
      test1.field("unique_hash", "test1");
      test1.field("notunique_hash", "test1");
      test1.save();

      final ODocument test2 = new ODocument("DistributedIndexTest");
      test2.field("unique", "test2");
      test2.field("notunique", "test2");
      test2.field("dictionary", "test2");
      test2.field("unique_hash", "test2");
      test2.field("notunique_hash", "test2");
      test2.save();

      final ODocument test3 = new ODocument("DistributedIndexTest");
      test3.field("unique", "test2");
      test3.field("notunique", "test3");
      test3.field("dictionary", "test3");
      test3.field("unique_hash", "test3");
      test3.field("notunique_hash", "test3");
      try {
        test3.save();
        Assert.fail();
      } catch (Exception e) {
        // CHECK DB COHERENCY
        final Iterable<ODocument> result = db.command(new OCommandSQL("select count(*) from DistributedIndexTest")).execute();
        Assert.assertEquals(result.iterator().next().field("count"), 2l);
      }

      final ODocument test4 = new ODocument("DistributedIndexTest");
      test4.field("unique", "test4");
      test4.field("notunique", "test4");
      test4.field("dictionary", "test4");
      test4.field("unique_hash", "test2");
      test4.field("notunique_hash", "test4");
      try {
        test4.save();
        Assert.fail();
      } catch (Exception e) {
        // CHECK DB COHERENCY
        final Iterable<ODocument> result = db.command(new OCommandSQL("select count(*) from DistributedIndexTest")).execute();
        Assert.assertEquals(result.iterator().next().field("count"), 2l);
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.toString());
    }
  }

  private void testODistributedRecordLockedExceptionExceptionThrown() {
    OrientBaseGraph orientGraph = new OrientGraphNoTx("remote:localhost/" + getDatabaseName());
    orientGraph.command(new OCommandSQL("ALTER DATABASE custom strictSQL=false")).execute();
    orientGraph.shutdown();

    OrientBaseGraph orientGraph2 = new OrientGraphNoTx("remote:localhost/" + getDatabaseName());

    orientGraph2.createVertexType("Toy");
    final OrientVertexType vertexType = orientGraph2.getVertexType("Toy");

    vertexType.createProperty("name", OType.STRING);

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE INDEX ").append("Toy.name.index").append(" ON ");
    sb.append("Toy").append(" (").append("name");
    sb.append(" COLLATE CI");
    sb.append(") UNIQUE");

    orientGraph2.command(new OCommandSQL(sb.toString())).execute();

    final OrientGraphFactory orientGraphFactory = new OrientGraphFactory("remote:localhost/" + getDatabaseName()).setupPool(1, 10);

    final OrientGraph orientGraph1 = orientGraphFactory.getTx();

    orientGraph1.addVertex("class:Toy", "name", "toy1");

    orientGraph1.commit();
    orientGraph1.shutdown();

    final OrientGraph orientGraph3 = orientGraphFactory.getTx();
    try {
      orientGraph3.addVertex("class:Toy", "name", "toy1");

      orientGraph3.commit();
    } catch (ORecordDuplicatedException e) {

    } catch (Exception e) {
      fail("Expected ORecordDuplicatedException but got " + e.getClass().toString());
    } finally {
      orientGraph3.shutdown();
    }

    final OrientGraph orientGraph4 = orientGraphFactory.getTx();

    orientGraph4.command(new OCommandSQL("insert into Toy ('name') values ('toy2')")).execute();

    orientGraph4.commit();
    orientGraph4.shutdown();

    final OrientGraph orientGraph5 = orientGraphFactory.getTx();
    try {
      orientGraph5.command(new OCommandSQL("insert into Toy ('name') values ('toy2')")).execute();

      orientGraph5.commit();
    } catch (ORecordDuplicatedException e) {

    } catch (Exception e) {
      fail("Expected ORecordDuplicatedException but got " + e.getClass().toString());
    } finally {
      orientGraph5.shutdown();
    }
  }

}
