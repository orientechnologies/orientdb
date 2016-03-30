/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be because the order of clusters could
 * be affected due to adding or removing cluster from storage.
 */
@Test(groups = "sql-select")
@SuppressWarnings("unchecked")
public class SQLSelectTest extends AbstractSelectTest {
  private ODocument record = new ODocument();

  @Parameters(value = "url")
  public SQLSelectTest(@Optional String url) {
    super(url);
  }

  private static void createAndLink(final OrientGraph graph, String name, Map<String, String> map, ODocument root) {
    OrientVertex vertex = graph.addVertex("class:vertexB", "name", name, "map", map);

    graph.addEdge(null, graph.getVertex(root), vertex, "E");
  }

  @Test
  public void queryNoDirtyResultset() {
    List<ODocument> result = executeQuery(" select from Profile ", database);

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertFalse(d.isDirty());
    }
  }

  @Test
  public void queryNoWhere() {
    List<ODocument> result = executeQuery(" select from Profile ", database);

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryParentesisAsRight() {
    List<ODocument> result = executeQuery(
        "  select from Profile where ( name = 'Giuseppe' and ( name <> 'Napoleone' and nick is not null ))  ", database);

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void querySingleAndDoubleQuotes() {
    List<ODocument> result = executeQuery("select from Profile where name = 'Giuseppe'", database);

    final int count = result.size();
    Assert.assertTrue(result.size() != 0);

    result = executeQuery("select from Profile where name = \"Giuseppe\"", database);
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.size(), count);
  }

  @Test
  public void queryTwoParentesisConditions() {
    List<ODocument> result = executeQuery(
        "select from Profile  where ( name = 'Giuseppe' and nick is not null ) or ( name = 'Napoleone' and nick is not null ) ",
        database);

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void testQueryCount() {
    database.getMetadata().reload();
    final long vertexesCount = database.countClass("V");
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select count(*) from V"));
    Assert.assertEquals(result.get(0).field("count"), vertexesCount);
  }

  @Test
  public void querySchemaAndLike() {
    List<ODocument> result1 = executeQuery("select * from cluster:profile where name like 'Gi%'", database);

    for (int i = 0; i < result1.size(); ++i) {
      record = result1.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().startsWith("Gi"));
    }

    List<ODocument> result2 = executeQuery("select * from cluster:profile where name like '%epp%'", database);

    Assert.assertEquals(result1, result2);

    List<ODocument> result3 = executeQuery("select * from cluster:profile where name like 'Gius%pe'", database);

    Assert.assertEquals(result1, result3);

    result1 = executeQuery("select * from cluster:profile where name like '%Gi%'", database);

    for (int i = 0; i < result1.size(); ++i) {
      record = result1.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().contains("Gi"));
    }

    result1 = executeQuery("select * from cluster:profile where name like ?", database, "%Gi%");

    for (int i = 0; i < result1.size(); ++i) {
      record = result1.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().contains("Gi"));
    }
  }

  @Test
  public void queryContainsInEmbeddedSet() {
    Set<String> tags = new HashSet<String>();
    tags.add("smart");
    tags.add("nice");

    ODocument doc = new ODocument("Profile");
    doc.field("tags", tags, OType.EMBEDDEDSET);

    doc.save();

    List<ODocument> resultset = executeQuery("select from Profile where tags CONTAINS 'smart'", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset = executeQuery("select from Profile where tags[0-1]  CONTAINSALL ['smart','nice']", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    doc.delete();
  }

  @Test
  public void queryContainsInEmbeddedList() {
    List<String> tags = new ArrayList<String>();
    tags.add("smart");
    tags.add("nice");

    ODocument doc = new ODocument("Profile");
    doc.field("tags", tags);

    doc.save();

    List<ODocument> resultset = executeQuery("select from Profile where tags[0] = 'smart'", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset = executeQuery("select from Profile where tags[0,1] CONTAINSALL ['smart','nice']", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset = executeQuery("select from Profile where tags[0-1] CONTAINSALL ['smart','nice']", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    doc.delete();
  }

  @Test
  public void queryContainsInDocumentSet() {
    HashSet<ODocument> coll = new HashSet<ODocument>();
    coll.add(new ODocument("name", "Luca", "surname", "Garulli"));
    coll.add(new ODocument("name", "Jay", "surname", "Miner"));

    ODocument doc = new ODocument("Profile");
    doc.field("coll", coll, OType.EMBEDDEDSET);

    doc.save();

    List<ODocument> resultset = executeQuery("select coll[name='Jay'] as value from Profile where coll is not null", database);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).field("value").getClass(), ODocument.class);
    Assert.assertEquals(((ODocument) resultset.get(0).field("value")).field("name"), "Jay");

    doc.delete();
  }

  @Test
  public void queryContainsInDocumentList() {
    List<ODocument> coll = new ArrayList<ODocument>();
    coll.add(new ODocument("name", "Luca", "surname", "Garulli"));
    coll.add(new ODocument("name", "Jay", "surname", "Miner"));

    ODocument doc = new ODocument("Profile");
    doc.field("coll", coll, OType.EMBEDDEDLIST);

    doc.save();

    List<ODocument> resultset = executeQuery("select coll[name='Jay'] as value from Profile where coll is not null", database);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).field("value").getClass(), ODocument.class);
    Assert.assertEquals(((ODocument) resultset.get(0).field("value")).field("name"), "Jay");

    doc.delete();
  }

  @Test
  public void queryContainsInEmbeddedMapClassic() {
    Map<String, ODocument> customReferences = new HashMap<String, ODocument>();
    customReferences.put("first", new ODocument("name", "Luca", "surname", "Garulli"));
    customReferences.put("second", new ODocument("name", "Jay", "surname", "Miner"));

    ODocument doc = new ODocument("Profile");
    doc.field("customReferences", customReferences, OType.EMBEDDEDMAP);

    doc.save();

    List<ODocument> resultset = executeQuery("select from Profile where customReferences CONTAINSKEY 'first'", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset = executeQuery("select from Profile where customReferences CONTAINSVALUE ( name like 'Ja%')", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset = executeQuery("select from Profile where customReferences[second]['name'] like 'Ja%'", database);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset = executeQuery("select customReferences['second', 'first'] from Profile where customReferences.size() = 2", database);
    Assert.assertEquals(resultset.size(), 1);

    if (resultset.get(0).field("customReferences").getClass().isArray()) {
      Object[] customReferencesBack = resultset.get(0).field("customReferences");
      Assert.assertEquals(customReferencesBack.length, 2);
      Assert.assertTrue(customReferencesBack[0] instanceof ODocument);
      Assert.assertTrue(customReferencesBack[1] instanceof ODocument);
    } else if (resultset.get(0).field("customReferences") instanceof List) {
      List<ODocument> customReferencesBack = resultset.get(0).field("customReferences");
      Assert.assertEquals(customReferencesBack.size(), 2);
      Assert.assertTrue(customReferencesBack.get(0) instanceof ODocument);
      Assert.assertTrue(customReferencesBack.get(1) instanceof ODocument);
    } else
      Assert.assertTrue(false, "Wrong type received: " + resultset.get(0).field("customReferences"));

    resultset = executeQuery(
        "select customReferences[second]['name'] from Profile where customReferences[second]['name'] is not null", database);
    Assert.assertEquals(resultset.size(), 1);

    resultset = executeQuery(
        "select customReferences[second]['name'] as value from Profile where customReferences[second]['name'] is not null",
        database);
    Assert.assertEquals(resultset.size(), 1);

    doc.delete();
  }

  @Test
  public void queryContainsInEmbeddedMapNew() {
    Map<String, ODocument> customReferences = new HashMap<String, ODocument>();
    customReferences.put("first", new ODocument("name", "Luca", "surname", "Garulli"));
    customReferences.put("second", new ODocument("name", "Jay", "surname", "Miner"));

    ODocument doc = new ODocument("Profile");
    doc.field("customReferences", customReferences, OType.EMBEDDEDMAP);

    doc.save();

    List<ODocument> resultset = executeQuery("select from Profile where customReferences.keys() CONTAINS 'first'", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset = executeQuery("select from Profile where customReferences.values() CONTAINS ( name like 'Ja%')", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    doc.delete();
  }

  @Test
  public void queryCollectionContainsLowerCaseSubStringIgnoreCase() {
    List<ODocument> result = executeQuery(
        "select * from cluster:profile where races contains (name.toLowerCase().subString(0,1) = 'e')", database);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertNotNull(record.field("races"));

      Collection<ODocument> races = record.field("races");
      boolean found = false;
      for (ODocument race : races) {
        if (((String) race.field("name")).toLowerCase().substring(0, 1).equals("e")) {
          found = true;
          break;
        }
      }
      Assert.assertTrue(found);
    }
  }

  @Test
  public void queryCollectionContainsInRecords() {
    record.reset();
    record.setClassName("Animal");
    record.field("name", "Cat");

    Collection<ODocument> races = new HashSet<ODocument>();
    races.add(((ODocument) database.newInstance("AnimalRace")).field("name", "European"));
    races.add(((ODocument) database.newInstance("AnimalRace")).field("name", "Siamese"));
    record.field("age", 10);
    record.field("races", races);
    record.save();

    List<ODocument> result = executeQuery("select * from cluster:animal where races contains (name in ['European','Asiatic'])",
        database);

    boolean found = false;
    for (int i = 0; i < result.size() && !found; ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("races"));

      races = record.field("races");
      for (ODocument race : races) {
        if (((String) race.field("name")).equals("European") || ((String) race.field("name")).equals("Asiatic")) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result = executeQuery("select * from cluster:animal where races contains (name in ['Asiatic','European'])", database);

    found = false;
    for (int i = 0; i < result.size() && !found; ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("races"));

      races = record.field("races");
      for (ODocument race : races) {
        if (((String) race.field("name")).equals("European") || ((String) race.field("name")).equals("Asiatic")) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result = executeQuery("select * from cluster:animal where races contains (name in ['aaa','bbb'])", database);
    Assert.assertEquals(result.size(), 0);

    result = executeQuery("select * from cluster:animal where races containsall (name in ['European','Asiatic'])", database);
    Assert.assertEquals(result.size(), 0);

    result = executeQuery("select * from cluster:animal where races containsall (name in ['European','Siamese'])", database);
    Assert.assertEquals(result.size(), 1);

    result = executeQuery("select * from cluster:animal where races containsall (age < 100) LIMIT 1000 SKIP 0", database);
    Assert.assertEquals(result.size(), 0);

    result = executeQuery("select * from cluster:animal where not ( races contains (age < 100) ) LIMIT 20 SKIP 0", database);
    Assert.assertEquals(result.size(), 1);

    record.delete();
  }

  @Test
  public void queryCollectionInNumbers() {
    record.reset();
    record.setClassName("Animal");
    record.field("name", "Cat");

    Collection<Integer> rates = new HashSet<Integer>();
    rates.add(100);
    rates.add(200);
    record.field("rates", rates);

    record.save();

    List<ODocument> result = executeQuery("select * from cluster:animal where rates in [100,200]", database);

    boolean found = false;
    for (int i = 0; i < result.size() && !found; ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("rates"));

      rates = record.field("rates");
      for (Integer rate : rates) {
        if (rate == 100 || rate == 105) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result = executeQuery("select * from cluster:animal where rates in [200,10333]", database);

    found = false;
    for (int i = 0; i < result.size() && !found; ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("rates"));

      rates = record.field("rates");
      for (Integer rate : rates) {
        if (rate == 100 || rate == 105) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result = executeQuery("select * from cluster:animal where rates contains 500", database);
    Assert.assertEquals(result.size(), 0);

    result = executeQuery("select * from cluster:animal where rates contains 100", database);
    Assert.assertEquals(result.size(), 1);

    record.delete();
  }

  @Test
  public void queryWhereRidDirectMatching() {
    List<Long> positions = getValidPositions(4);

    List<ODocument> result = executeQuery("select * from OUser where roles contains #4:" + positions.get(0), database);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryWhereInpreparred() {
    List<ODocument> result = executeQuery("select * from OUser where name in [ :name ]", database, "admin");

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((ODocument) result.get(0).getRecord()).field("name"), "admin");
  }

  @Test
  public void queryInAsParameter() {
    List<ODocument> roles = executeQuery("select from orole limit 1", database);

    List<ODocument> result = executeQuery("select * from OUser where roles in ?", database, roles);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryAnyOperator() {
    List<ODocument> result = executeQuery("select from Profile where any() like 'N%'", database);

    Assert.assertTrue(result.size() > 0);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));

      boolean found = false;
      for (Object fieldValue : record.fieldValues()) {
        if (fieldValue != null && fieldValue.toString().startsWith("N")) {
          found = true;
          break;
        }
      }
      Assert.assertTrue(found);
    }
  }

  @Test
  public void queryAllOperator() {
    List<ODocument> result = executeQuery("select from Account where all() is null", database);

    Assert.assertTrue(result.size() == 0);
  }

  @Test
  public void queryOrderBy() {
    List<ODocument> result = executeQuery("select from Profile order by name", database);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    boolean isNullSegment = true; // NULL VALUES AT THE BEGINNING!
    for (ODocument d : result) {
      final String fieldValue = d.field("name");
      if (fieldValue != null)
        isNullSegment = false;
      else
        Assert.assertTrue(isNullSegment);

      if (lastName != null && fieldValue != null)
        Assert.assertTrue(fieldValue.compareTo(lastName) >= 0);
      lastName = fieldValue;
    }
  }

  @Test
  public void queryOrderByWrongSyntax() {
    try {
      executeQuery("select from Profile order by name aaaa", database);
      Assert.fail();
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OCommandSQLParsingException);
    } catch (OCommandSQLParsingException e) {
    }
  }

  @Test
  public void queryLimitOnly() {
    List<ODocument> result = executeQuery("select from Profile limit 1", database);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void querySkipOnly() {
    List<ODocument> result = executeQuery("select from Profile", database);
    int total = result.size();

    result = executeQuery("select from Profile skip 1", database);
    Assert.assertEquals(result.size(), total - 1);
  }

  @Test
  public void queryPaginationWithSkipAndLimit() {
    List<ODocument> result = executeQuery("select from Profile", database);

    List<ODocument> page = executeQuery("select from Profile skip 10 limit 10", database);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals((Object) page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryOffsetOnly() {
    List<ODocument> result = executeQuery("select from Profile", database);
    int total = result.size();

    result = executeQuery("select from Profile offset 1", database);
    Assert.assertEquals(result.size(), total - 1);
  }

  @Test
  public void queryPaginationWithOffsetAndLimit() {
    List<ODocument> result = executeQuery("select from Profile", database);

    List<ODocument> page = executeQuery("select from Profile offset 10 limit 10", database);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals((Object) page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryPaginationWithOrderBySkipAndLimit() {
    List<ODocument> result = executeQuery("select from Profile order by name", database);

    List<ODocument> page = executeQuery("select from Profile order by name limit 10 skip 10", database);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals((Object) page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryPaginationWithOrderByDescSkipAndLimit() {
    List<ODocument> result = executeQuery("select from Profile order by name desc", database);

    List<ODocument> page = executeQuery("select from Profile order by name desc limit 10 skip 10", database);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals((Object) page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryOrderByAndLimit() {
    List<ODocument> result = executeQuery("select from Profile order by name limit 2", database);

    Assert.assertTrue(result.size() <= 2);

    String lastName = null;
    for (ODocument d : result) {
      if (lastName != null && d.field("name") != null)
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) >= 0);
      lastName = d.field("name");
    }
  }

  @Test
  public void queryConditionAndOrderBy() {
    List<ODocument> result = executeQuery("select from Profile where name is not null order by name", database);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    for (ODocument d : result) {
      if (lastName != null && d.field("name") != null)
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) >= 0);
      lastName = d.field("name");
    }
  }

  @Test
  public void queryConditionsAndOrderBy() {
    List<ODocument> result = executeQuery("select from Profile where name is not null order by name desc, id asc", database);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    for (ODocument d : result) {
      if (lastName != null && d.field("name") != null)
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) <= 0);
      lastName = d.field("name");
    }
  }

  @Test
  public void queryRecordTargetRid() {
    int profileClusterId = database.getMetadata().getSchema().getClass("Profile").getDefaultClusterId();
    List<Long> positions = getValidPositions(profileClusterId);

    List<ODocument> result = executeQuery("select from " + profileClusterId + ":" + positions.get(0), database);

    Assert.assertEquals(result.size(), 1);

    for (ODocument d : result) {
      Assert.assertEquals(d.getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(0));
    }
  }

  @Test
  public void queryRecordTargetRids() {
    int profileClusterId = database.getMetadata().getSchema().getClass("Profile").getDefaultClusterId();
    List<Long> positions = getValidPositions(profileClusterId);

    List<ODocument> result = executeQuery(
        " select from [" + profileClusterId + ":" + positions.get(0) + ", " + profileClusterId + ":" + positions.get(1) + "]",
        database);

    Assert.assertEquals(result.size(), 2);

    Assert.assertEquals(result.get(0).getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(0));
    Assert.assertEquals(result.get(1).getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(1));
  }

  @Test
  public void queryRecordAttribRid() {

    int profileClusterId = database.getMetadata().getSchema().getClass("Profile").getDefaultClusterId();
    List<Long> postions = getValidPositions(profileClusterId);

    List<ODocument> result = executeQuery("select from Profile where @rid = #" + profileClusterId + ":" + postions.get(0),
        database);

    Assert.assertEquals(result.size(), 1);

    for (ODocument d : result) {
      Assert.assertEquals(d.getIdentity().toString(), "#" + profileClusterId + ":" + postions.get(0));
    }
  }

  @Test
  public void queryRecordAttribClass() {
    List<ODocument> result = executeQuery("select from Profile where @class = 'Profile'", database);

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertEquals(d.getClassName(), "Profile");
    }
  }

  @Test
  public void queryRecordAttribVersion() {
    List<ODocument> result = executeQuery("select from Profile where @version > 0", database);

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertTrue(d.getRecordVersion().getCounter() > 0);
    }
  }

  @Test
  public void queryRecordAttribSize() {
    List<ODocument> result = executeQuery("select from Profile where @size >= 50", database);

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertTrue(d.toStream().length >= 50);
    }
  }

  @Test
  public void queryRecordAttribType() {
    List<ODocument> result = executeQuery("select from Profile where @type = 'document'", database);

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryWrongOperator() {
    try {
      executeQuery("select from Profile where name like.toLowerCase() '%Jay%'", database);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void queryEscaping() {
    executeQuery("select from Profile where name like '%\\'Jay%'", database);
  }

  @Test
  public void queryWithLimit() {
    Assert.assertEquals(executeQuery("select from Profile limit 3", database).size(), 3);
  }

  @SuppressWarnings("unused")
  @Test
  public void testRecordNumbers() {
    long tot = database.countClass("V");

    int count = 0;
    for (ODocument record : database.browseClass("V")) {
      count++;
    }

    Assert.assertEquals(count, tot);

    Assert.assertTrue(executeQuery("select from V", database).size() >= tot);
  }

  @Test
  public void queryWithManualPagination() {
    ORID last = new ORecordId();
    List<ODocument> resultset = executeQuery("select from Profile where @rid > ? LIMIT 3", database, last);

    int iterationCount = 0;
    Assert.assertTrue(!resultset.isEmpty());
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (ODocument d : resultset) {
        Assert.assertTrue(d.getIdentity().getClusterId() < 0 || (d.getIdentity().getClusterId() >= last.getClusterId())
            && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = executeQuery("select from Profile where @rid > ? LIMIT 3", database, last);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPagination() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile LIMIT 3");
    ORID last = new ORecordId();

    List<ODocument> resultset = database.query(query);

    int iterationCount = 0;
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (ODocument d : resultset) {
        Assert.assertTrue(d.getIdentity().getClusterId() >= last.getClusterId()
            && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = database.query(query);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationAndRidInWhere() {
    int clusterId = database.getClusterIdByName("profile");

    long[] range = database.getStorage().getClusterDataRange(clusterId);

    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where @rid between #" + clusterId
        + ":" + range[0] + " and #" + clusterId + ":" + range[1] + " LIMIT 3");

    ORID last = new ORecordId();

    List<ODocument> resultset = database.query(query);

    Assert.assertEquals(resultset.get(0).getIdentity(), new ORecordId(clusterId, range[0]));

    int iterationCount = 0;
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (ODocument d : resultset) {
        Assert.assertTrue(d.getIdentity().getClusterId() >= last.getClusterId()
            && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = database.query(query);
    }

    Assert.assertEquals(last, new ORecordId(clusterId, range[1]));
    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhere() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from Profile where followers.length() > 0 LIMIT 3");
    ORID last = new ORecordId();

    List<ODocument> resultset = database.query(query);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (ODocument d : resultset) {
        Assert.assertTrue(d.getIdentity().getClusterId() >= last.getClusterId()
            && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      // System.out.printf("\nIterating page %d, last record is %s", iterationCount, last);

      iterationCount++;
      resultset = database.query(query);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhereAndBindingVar() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from Profile where followers.length() > ? LIMIT 3");
    ORID last = new ORecordId();

    List<ODocument> resultset = database.query(query, 0);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (ODocument d : resultset) {
        Assert.assertTrue(d.getIdentity().getClusterId() >= last.getClusterId()
            && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = database.query(query, 0);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhereAndBindingVarAtTheFirstQueryCall() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from Profile where followers.length() > ? LIMIT 3");
    ORID last = new ORecordId();

    List<ODocument> resultset = database.query(query, 0);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (ODocument d : resultset) {
        Assert.assertTrue(d.getIdentity().getClusterId() >= last.getClusterId()
            && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = database.query(query, 0);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAbsenceOfAutomaticPaginationBecauseOfBindingVarReset() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from Profile where followers.length() > ? LIMIT 3");

    List<ODocument> resultset = database.query(query, -1);

    final ORID firstRidFirstQuery = resultset.get(0).getIdentity();

    resultset = database.query(query, -2);

    final ORID firstRidSecondQueryQuery = resultset.get(0).getIdentity();

    Assert.assertEquals(firstRidFirstQuery, firstRidSecondQueryQuery);
  }

  @Test
  public void includeFields() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select expand( roles.include('name') ) from OUser");

    List<ODocument> resultset = database.query(query);

    for (ODocument d : resultset) {
      Assert.assertTrue(d.fields() <= 1);
      if (d.fields() == 1)
        Assert.assertTrue(d.containsField("name"));
    }
  }

  @Test
  public void excludeFields() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select expand( roles.exclude('rules') ) from OUser");

    List<ODocument> resultset = database.query(query);

    for (ODocument d : resultset) {
      Assert.assertFalse(d.containsField("rules"));
    }
  }

  @Test
  public void excludeAttributes() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select expand( roles.exclude('@rid', '@class') ) from OUser");

    List<ODocument> resultset = database.query(query);

    for (ODocument d : resultset) {
      Assert.assertFalse(d.getIdentity().isPersistent());
      Assert.assertNull(d.getSchemaClass());
    }
  }

  @Test
  public void queryResetPagination() {
    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile LIMIT 3");

    List<ODocument> resultset = database.query(query);
    final ORID firstRidFirstQuery = resultset.get(0).getIdentity();
    query.resetPagination();

    resultset = database.query(query);
    final ORID firstRidSecondQueryQuery = resultset.get(0).getIdentity();

    Assert.assertEquals(firstRidFirstQuery, firstRidSecondQueryQuery);
  }

  @Test
  public void queryBetween() {
    List<ODocument> result = executeQuery("select * from account where nr between 10 and 20", database);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);

      Assert.assertTrue(((Integer) record.field("nr")) >= 10 && ((Integer) record.field("nr")) <= 20);
    }
  }

  @Test
  public void queryParenthesisInStrings() {
    Assert.assertNotNull(database.command(new OCommandSQL("INSERT INTO account (name) VALUES ('test (demo)')")).execute());

    List<ODocument> result = executeQuery("select * from account where name = 'test (demo)'", database);

    Assert.assertEquals(result.size(), 1);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);
      Assert.assertEquals(record.field("name"), "test (demo)");
    }

  }

  @Test
  public void queryMathOperators() {

    List<ODocument> result = executeQuery("select * from account where id < 3 + 4", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i)
      Assert.assertTrue(((Integer) result.get(i).field("id")) < 3 + 4);

    result = executeQuery("select * from account where id < 10 - 3", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i)
      Assert.assertTrue(((Integer) result.get(i).field("id")) < 10 - 3);

    result = executeQuery("select * from account where id < 3 * 2", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i)
      Assert.assertTrue(((Integer) result.get(i).field("id")) < 3 * 2);

    result = executeQuery("select * from account where id < 120 / 20", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i)
      Assert.assertTrue(((Integer) result.get(i).field("id")) < 120 / 20);

    result = executeQuery("select * from account where id < 27 % 10", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i)
      Assert.assertTrue(((Integer) result.get(i).field("id")) < 27 % 10);

    result = executeQuery("select * from account where id = id * 1", database);
    Assert.assertFalse(result.isEmpty());

    List<ODocument> result2 = executeQuery("select count(*) as tot from account where id >= 0", database);
    Assert.assertEquals(result.size(), ((Number) result2.get(0).field("tot")).intValue());

  }

  @Test
  public void testBetweenWithParameters() {

    final List<ODocument> result = executeQuery("select * from company where id between ? and ?", database, 4, 7);
    Assert.assertEquals(result.size(), 4);

    final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
    for (final ODocument record : result) {
      resultsList.remove(record.<Integer> field("id"));
    }

  }

  @Test
  public void testInWithParameters() {

    final List<ODocument> result = executeQuery("select * from company where id in [?, ?, ?, ?]", database, 4, 5, 6, 7);

    Assert.assertEquals(result.size(), 4);

    final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
    for (final ODocument record : result) {
      resultsList.remove(record.<Integer> field("id"));
    }

  }

  @Test
  public void testEqualsNamedParameter() {

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", 4);
    final List<ODocument> result = executeQuery("select * from company where id = :id", database, params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testQueryAsClass() {

    List<ODocument> result = executeQuery("select from Account where addresses.@class in [ 'Address' ]", database);
    Assert.assertFalse(result.isEmpty());
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("addresses"));
      Assert.assertEquals(
          ((ODocument) ((Collection<OIdentifiable>) d.field("addresses")).iterator().next().getRecord()).getSchemaClass().getName(),
          "Address");
    }
  }

  @Test
  public void testQueryNotOperator() {

    List<ODocument> result = executeQuery("select from Account where not ( addresses.@class in [ 'Address' ] )", database);
    Assert.assertFalse(result.isEmpty());
    for (ODocument d : result) {
      Assert.assertTrue(d.field("addresses") == null || ((Collection<OIdentifiable>) d.field("addresses")).isEmpty()
          || !((ODocument) ((Collection<OIdentifiable>) d.field("addresses")).iterator().next().getRecord()).getSchemaClass()
              .getName().equals("Address"));
    }
  }

  @Test
  public void testSquareBracketsOnCondition() {
    List<ODocument> result = executeQuery("select from Account where addresses[@class='Address'][city.country.name] = 'Washington'",
        database);
    Assert.assertFalse(result.isEmpty());
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("addresses"));
      Assert.assertEquals(
          ((ODocument) ((Collection<OIdentifiable>) d.field("addresses")).iterator().next().getRecord()).getSchemaClass().getName(),
          "Address");
    }
  }

  public void testParams() {
    OClass test = database.getMetadata().getSchema().getClass("test");
    if (test == null) {
      test = database.getMetadata().getSchema().createClass("test");
      test.createProperty("f1", OType.STRING);
      test.createProperty("f2", OType.STRING);
    }
    ODocument document = new ODocument(test);
    document.field("f1", "a").field("f2", "a");
    database.save(document);

    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("p1", "a");
    database.query(new OSQLSynchQuery<ODocument>("select from test where (f1 = :p1)"), parameters);
    database.query(new OSQLSynchQuery<ODocument>("select from test where f1 = :p1 and f2 = :p1"), parameters);
  }

  @Test
  public void queryInstanceOfOperator() {
    List<ODocument> result = executeQuery("select from Account", database);

    Assert.assertTrue(result.size() != 0);

    List<ODocument> result2 = executeQuery("select from Account where @this instanceof 'Account'", database);

    Assert.assertEquals(result2.size(), result.size());

    List<ODocument> result3 = executeQuery("select from Account where @class instanceof 'Account'", database);

    Assert.assertEquals(result3.size(), result.size());

  }

  @Test
  public void subQuery() {
    List<ODocument> result = executeQuery(
        "select from Account where name in ( select name from Account where name is not null limit 1 )", database);

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void subQueryNoFrom() {
    List<ODocument> result2 = executeQuery(
        "select $names let $names = (select EXPAND( addresses.city ) as city from Account where addresses.size() > 0 )", database);

    Assert.assertTrue(result2.size() != 0);
    Assert.assertTrue(result2.get(0).field("$names") instanceof Collection<?>);
    Assert.assertFalse(((Collection<?>) result2.get(0).field("$names")).isEmpty());
  }

  @Test
  public void subQueryLetAndIndexedWhere() {
    List<ODocument> result = executeQuery("select $now from OUser let $now = eval('42') where name = 'admin'", database);

    Assert.assertEquals(result.size(), 1);
    Assert.assertNotNull(result.get(0).field("$now"), result.get(0).toString());
  }

  @Test
  public void queryOrderByWithLimit() {

    OSchema schema = database.getMetadata().getSchema();
    OClass facClass = schema.getClass("FicheAppelCDI");
    if (facClass == null) {
      facClass = schema.createClass("FicheAppelCDI");
    }
    if (!facClass.existsProperty("date")) {
      facClass.createProperty("date", OType.DATE);
    }

    final Calendar currentYear = Calendar.getInstance();
    final Calendar oneYearAgo = Calendar.getInstance();
    oneYearAgo.add(Calendar.YEAR, -1);

    ODocument doc1 = new ODocument(facClass);
    doc1.field("context", "test");
    doc1.field("date", currentYear.getTime());
    doc1.save();

    ODocument doc2 = new ODocument(facClass);
    doc2.field("context", "test");
    doc2.field("date", oneYearAgo.getTime());
    doc2.save();

    List<ODocument> result = database
        .query(new OSQLSynchQuery<ODocument>("select * from " + facClass.getName() + " where context = 'test' order by date", 1));

    Calendar smaller = Calendar.getInstance();
    smaller.setTime((Date) result.get(0).field("date", Date.class));
    Assert.assertEquals(smaller.get(Calendar.YEAR), oneYearAgo.get(Calendar.YEAR));

    result = database.query(
        new OSQLSynchQuery<ODocument>("select * from " + facClass.getName() + " where context = 'test' order by date DESC", 1));

    Calendar bigger = Calendar.getInstance();
    bigger.setTime((Date) result.get(0).field("date", Date.class));
    Assert.assertEquals(bigger.get(Calendar.YEAR), currentYear.get(Calendar.YEAR));
  }

  @Test
  public void queryWithTwoRidInWhere() {
    int clusterId = database.getClusterIdByName("profile");

    List<Long> positions = getValidPositions(clusterId);

    final long minPos;
    final long maxPos;
    if (positions.get(5).compareTo(positions.get(25)) > 0) {
      minPos = positions.get(25);
      maxPos = positions.get(5);
    } else {
      minPos = positions.get(5);
      maxPos = positions.get(25);
    }

    List<ODocument> resultset = executeQuery("select @rid.trim() as oid, name from Profile where (@rid in [#" + clusterId + ":"
        + positions.get(5) + "] or @rid in [#" + clusterId + ":" + positions.get(25) + "]) AND @rid > ? LIMIT 10000", database,
        new ORecordId(clusterId, minPos));

    Assert.assertEquals(resultset.size(), 1);

    Assert.assertEquals(resultset.get(0).field("oid"), new ORecordId(clusterId, maxPos).toString());
  }

  @Test
  public void testSelectFromListParameter() {
    OClass placeClass = database.getMetadata().getSchema().createClass("Place");
    placeClass.createProperty("id", OType.STRING);
    placeClass.createProperty("descr", OType.STRING);
    placeClass.createIndex("place_id_index", INDEX_TYPE.UNIQUE, "id");

    ODocument odoc = new ODocument("Place");
    odoc.field("id", "adda");
    odoc.field("descr", "Adda");
    database.save(odoc);

    odoc = new ODocument("Place");
    odoc.field("id", "lago_di_como");
    odoc.field("descr", "Lago di Como");
    database.save(odoc);

    Map<String, Object> params = new HashMap<String, Object>();
    List<String> inputValues = new ArrayList<String>();
    inputValues.add("lago_di_como");
    inputValues.add("lecco");
    params.put("place", inputValues);

    List<ODocument> result = executeQuery("select from place where id in :place", database, params);
    Assert.assertEquals(1, result.size());

    database.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testSelectRidFromListParameter() {
    OClass placeClass = database.getMetadata().getSchema().createClass("Place");
    placeClass.createProperty("id", OType.STRING);
    placeClass.createProperty("descr", OType.STRING);
    placeClass.createIndex("place_id_index", INDEX_TYPE.UNIQUE, "id");

    List<ORID> inputValues = new ArrayList<ORID>();

    ODocument odoc = new ODocument("Place");
    odoc.field("id", "adda");
    odoc.field("descr", "Adda");
    database.save(odoc);
    inputValues.add(odoc.getIdentity());

    odoc = new ODocument("Place");
    odoc.field("id", "lago_di_como");
    odoc.field("descr", "Lago di Como");
    database.save(odoc);
    inputValues.add(odoc.getIdentity());

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("place", inputValues);

    List<ODocument> result = executeQuery("select from place where @rid in :place", database, params);
    Assert.assertEquals(2, result.size());

    database.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testSelectRidInList() {
    OClass placeClass = database.getMetadata().getSchema().createClass("Place");
    database.getMetadata().getSchema().createClass("FamousPlace", placeClass);

    ODocument firstPlace = new ODocument("Place");
    database.save(firstPlace);
    ODocument secondPlace = new ODocument("Place");
    database.save(secondPlace);
    ODocument famousPlace = new ODocument("FamousPlace");
    database.save(famousPlace);

    ORID secondPlaceId = secondPlace.getIdentity();
    ORID famousPlaceId = famousPlace.getIdentity();
    // if one of these two asserts fails, the test will be meaningless.
    Assert.assertTrue(secondPlaceId.getClusterId() < famousPlaceId.getClusterId());
    Assert.assertTrue(secondPlaceId.getClusterPosition() > famousPlaceId.getClusterPosition());

    List<ODocument> result = executeQuery("select from Place where @rid in [" + secondPlaceId + "," + famousPlaceId + "]",
        database);
    Assert.assertEquals(2, result.size());

    database.getMetadata().getSchema().dropClass("FamousPlace");
    database.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testMapKeys() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", 4);
    final List<ODocument> result = executeQuery("select * from company where id = :id", database, params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryAsynch() {
    final String sqlOne = "select * from company where id between 4 and 7";
    final String sqlTwo = "select $names let $names = (select EXPAND( addresses.city ) as city from Account where addresses.size() > 0 )";

    final List<ODocument> synchResultOne = database.command(new OSQLSynchQuery<ODocument>(sqlOne)).execute();
    final List<ODocument> synchResultTwo = database.command(new OSQLSynchQuery<ODocument>(sqlTwo)).execute();

    Assert.assertTrue(synchResultOne.size() > 0);
    Assert.assertTrue(synchResultTwo.size() > 0);

    final List<ODocument> asynchResultOne = new ArrayList<ODocument>();
    final List<ODocument> asynchResultTwo = new ArrayList<ODocument>();
    final AtomicBoolean endOneCalled = new AtomicBoolean();
    final AtomicBoolean endTwoCalled = new AtomicBoolean();

    database.command(new OSQLAsynchQuery<ODocument>(sqlOne, new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        asynchResultOne.add((ODocument) iRecord);
        return true;
      }

      @Override
      public void end() {
        endOneCalled.set(true);

        database.command(new OSQLAsynchQuery<ODocument>(sqlTwo, new OCommandResultListener() {
          @Override
          public boolean result(Object iRecord) {
            asynchResultTwo.add((ODocument) iRecord);
            return true;
          }

          @Override
          public void end() {
            endTwoCalled.set(true);
          }
        })).execute();
      }
    })).execute();

    Assert.assertTrue(endOneCalled.get());
    Assert.assertTrue(endTwoCalled.get());

    Assert.assertTrue(ODocumentHelper.compareCollections(database, synchResultTwo, database, asynchResultTwo, null));
    Assert.assertTrue(ODocumentHelper.compareCollections(database, synchResultOne, database, asynchResultOne, null));
  }

  @Test
  public void queryAsynchHalfForheFirstQuery() {
    final String sqlOne = "select * from company where id between 4 and 7";
    final String sqlTwo = "select $names let $names = (select EXPAND( addresses.city ) as city from Account where addresses.size() > 0 )";

    final List<ODocument> synchResultOne = database.command(new OSQLSynchQuery<ODocument>(sqlOne)).execute();
    final List<ODocument> synchResultTwo = database.command(new OSQLSynchQuery<ODocument>(sqlTwo)).execute();

    Assert.assertTrue(synchResultOne.size() > 0);
    Assert.assertTrue(synchResultTwo.size() > 0);

    final List<ODocument> asynchResultOne = new ArrayList<ODocument>();
    final List<ODocument> asynchResultTwo = new ArrayList<ODocument>();
    final AtomicBoolean endOneCalled = new AtomicBoolean();
    final AtomicBoolean endTwoCalled = new AtomicBoolean();

    database.command(new OSQLAsynchQuery<ODocument>(sqlOne, new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        asynchResultOne.add((ODocument) iRecord);
        return asynchResultOne.size() < synchResultOne.size() / 2;
      }

      @Override
      public void end() {
        endOneCalled.set(true);

        database.command(new OSQLAsynchQuery<ODocument>(sqlTwo, new OCommandResultListener() {
          @Override
          public boolean result(Object iRecord) {
            asynchResultTwo.add((ODocument) iRecord);
            return true;
          }

          @Override
          public void end() {
            endTwoCalled.set(true);
          }
        })).execute();
      }
    })).execute();

    Assert.assertTrue(endOneCalled.get());
    Assert.assertTrue(endTwoCalled.get());

    Assert.assertTrue(ODocumentHelper.compareCollections(database, synchResultOne.subList(0, synchResultOne.size() / 2), database,
        asynchResultOne, null));
    Assert.assertTrue(ODocumentHelper.compareCollections(database, synchResultTwo, database, asynchResultTwo, null));
  }

  @Test
  public void queryOrderByRidDesc() {
    List<ODocument> result = executeQuery("select from OUser order by @rid desc", database);

    Assert.assertFalse(result.isEmpty());

    ORID lastRid = null;
    for (ODocument d : result) {
      ORID rid = d.getIdentity();

      if (lastRid != null)
        Assert.assertTrue(rid.compareTo(lastRid) < 0);
      lastRid = rid;
    }

    ODocument res = database.command(new OCommandSQL("explain select from OUser order by @rid desc")).execute();
    Assert.assertNull(res.field("orderByElapsed"));

  }

  public void testSelectFromIndexValues() {
    database.command(new OCommandSQL("create index selectFromIndexValues on Profile (name) notunique")).execute();

    final List<ODocument> classResult = new ArrayList<ODocument>((List<ODocument>) database.query(
        new OSQLSynchQuery<ODocument>("select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name is not null)")));

    final List<ODocument> indexValuesResult = database.query(new OSQLSynchQuery<ODocument>(
        "select from indexvalues:selectFromIndexValues where ((nick like 'J%') or (nick like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (ODocument document : indexValuesResult) {
      String name = document.field("name");

      if (lastName != null)
        Assert.assertTrue(lastName.compareTo(name) <= 0);

      lastName = name;
      Assert.assertTrue(classResult.remove(document));
    }

    Assert.assertTrue(classResult.isEmpty());
  }

  public void testSelectFromIndexValuesAsc() {
    database.command(new OCommandSQL("create index selectFromIndexValuesAsc on Profile (name) notunique")).execute();

    final List<ODocument> classResult = new ArrayList<ODocument>((List<ODocument>) database.query(
        new OSQLSynchQuery<ODocument>("select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name is not null)")));

    final List<ODocument> indexValuesResult = database.query(new OSQLSynchQuery<ODocument>(
        "select from indexvaluesasc:selectFromIndexValuesAsc where ((nick like 'J%') or (nick like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (ODocument document : indexValuesResult) {
      String name = document.field("name");

      if (lastName != null)
        Assert.assertTrue(lastName.compareTo(name) <= 0);

      lastName = name;
      Assert.assertTrue(classResult.remove(document));
    }

    Assert.assertTrue(classResult.isEmpty());
  }

  public void testSelectFromIndexValuesDesc() {
    database.command(new OCommandSQL("create index selectFromIndexValuesDesc on Profile (name) notunique")).execute();

    final List<ODocument> classResult = new ArrayList<ODocument>((List<ODocument>) database.query(
        new OSQLSynchQuery<ODocument>("select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name is not null)")));

    final List<ODocument> indexValuesResult = database.query(new OSQLSynchQuery<ODocument>(
        "select from indexvaluesdesc:selectFromIndexValuesDesc where ((nick like 'J%') or (nick like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (ODocument document : indexValuesResult) {
      String name = document.field("name");

      if (lastName != null)
        Assert.assertTrue(lastName.compareTo(name) >= 0);

      lastName = name;
      Assert.assertTrue(classResult.remove(document));
    }

    Assert.assertTrue(classResult.isEmpty());
  }

  public void testQueryParameterNotPersistent() {
    ODocument doc = new ODocument();
    doc.field("test", "test");
    database.query(new OSQLSynchQuery<Object>("select from OUser where @rid = ?"), doc);
    Assert.assertTrue(doc.isDirty());

  }

  public void testQueryLetExecutedOnce() {
    final List<OIdentifiable> result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select name, $counter as counter from OUser let $counter = eval(\"$counter + 1\")"));

    Assert.assertFalse(result.isEmpty());
    int i = 1;
    for (OIdentifiable r : result) {
      Assert.assertEquals(((ODocument) r.getRecord()).field("counter"), i++);
    }
  }

  @Test
  public void testMultipleClustersWithPagination() throws Exception {
    final OClass cls = database.getMetadata().getSchema().createClass("PersonMultipleClusters");
    cls.addCluster("PersonMultipleClusters_1");
    cls.addCluster("PersonMultipleClusters_2");
    cls.addCluster("PersonMultipleClusters_3");
    cls.addCluster("PersonMultipleClusters_4");

    try {
      Set<String> names = new HashSet<String>(Arrays.asList(new String[] { "Luca", "Jill", "Sara", "Tania", "Gianluca", "Marco" }));
      for (String n : names) {
        new ODocument("PersonMultipleClusters").field("First", n).save();
      }

      OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from PersonMultipleClusters where @rid > ? limit 2");
      List<ODocument> resultset = database.query(query, new ORecordId());

      while (!resultset.isEmpty()) {
        final ORID last = resultset.get(resultset.size() - 1).getIdentity();

        for (ODocument personDoc : resultset) {
          Assert.assertTrue(names.contains(personDoc.field("First")));
          Assert.assertTrue(names.remove(personDoc.field("First")));
        }

        resultset = database.query(query, last);
      }

      Assert.assertTrue(names.isEmpty());

    } finally {
      database.getMetadata().getSchema().dropClass("PersonMultipleClusters");
    }
  }

  public void testOutFilterInclude() {
    database.command(new OCommandSQL("create class TestOutFilterInclude extends V")).execute();
    database.command(new OCommandSQL("create class linkedToOutFilterInclude extends E")).execute();
    database.command(new OCommandSQL("insert into TestOutFilterInclude content { \"name\": \"one\" }")).execute();
    database.command(new OCommandSQL("insert into TestOutFilterInclude content { \"name\": \"two\" }")).execute();
    database.command(new OCommandSQL("create edge linkedToOutFilterInclude from (select from TestOutFilterInclude where name = 'one') to (select from TestOutFilterInclude where name = 'two')")).execute();



    final List<OIdentifiable> result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select expand(out('linkedToOutFilterInclude')[@class='TestOutFilterInclude'].include('@rid')) from TestOutFilterInclude where name = 'one'"));

    Assert.assertEquals(result.size(), 1);

    for (OIdentifiable r : result) {
      Assert.assertEquals(((ODocument) r.getRecord()).field("name"), null);
    }
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final ORecordIteratorCluster<ODocument> iteratorCluster = database.browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 100; i++) {
      if (!iteratorCluster.hasNext())
        break;

      ODocument doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }

  public void testBinaryClusterSelect() {
    database.command(new OCommandSQL("create cluster binarycluster")).execute();
    database.reload();
    ORecordBytes bytes = new ORecordBytes(new byte[]{1,2,3});
    database.save(bytes, "binarycluster");


    List<OIdentifiable> result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select from cluster:binarycluster"));

    Assert.assertEquals(result.size(), 1);

    database.command(
        new OCommandSQL("delete from cluster:binarycluster")).execute();

    result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select from cluster:binarycluster"));

    Assert.assertEquals(result.size(), 0);
  }

  public void testExpandSkip() {
    OSchemaProxy schema = database.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    final OClass cls = schema.createClass("TestExpandSkip", v);
    cls.createProperty("name", OType.STRING);
    cls.createIndex("TestExpandSkip.name", INDEX_TYPE.UNIQUE, "name");
    database.command(new OCommandSQL("CREATE VERTEX TestExpandSkip set name = '1'")).execute();
    database.command(new OCommandSQL("CREATE VERTEX TestExpandSkip set name = '2'")).execute();
    database.command(new OCommandSQL("CREATE VERTEX TestExpandSkip set name = '3'")).execute();
    database.command(new OCommandSQL("CREATE VERTEX TestExpandSkip set name = '4'")).execute();

    database.command(new OCommandSQL("CREATE EDGE E FROM (SELECT FROM TestExpandSkip WHERE name = '1') to (SELECT FROM TestExpandSkip WHERE name <> '1')")).execute();

    List<OIdentifiable> result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select expand(out()) from TestExpandSkip where name = '1'"));
    Assert.assertEquals(result.size(), 3);

    result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select expand(out()) from TestExpandSkip where name = '1' skip 1"));
    Assert.assertEquals(result.size(), 2);

    result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select expand(out()) from TestExpandSkip where name = '1' skip 2"));
    Assert.assertEquals(result.size(), 1);

    result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select expand(out()) from TestExpandSkip where name = '1' skip 3"));
    Assert.assertEquals(result.size(), 0);

    result = database.query(
        new OSQLSynchQuery<OIdentifiable>("select expand(out()) from TestExpandSkip where name = '1' skip 1 limit 1"));
    Assert.assertEquals(result.size(), 1);

  }


}
