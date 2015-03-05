package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.List;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class IndexOnSubclassesTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public IndexOnSubclassesTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    database.command(new OCommandSQL("create class IndexInSubclassesTestBase")).execute();
    database.command(new OCommandSQL("create property IndexInSubclassesTestBase.name string")).execute();

    database.command(new OCommandSQL("create class IndexInSubclassesTestChild1 extends IndexInSubclassesTestBase")).execute();
    database.command(
        new OCommandSQL("create index IndexInSubclassesTestChild1.name on IndexInSubclassesTestChild1 (name) notunique")).execute();

    database.command(new OCommandSQL("create class IndexInSubclassesTestChild2 extends IndexInSubclassesTestBase")).execute();
    database.command(
        new OCommandSQL("create index IndexInSubclassesTestChild2.name on IndexInSubclassesTestChild2 (name) notunique")).execute();
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final OSchema schema = database.getMetadata().getSchema();
    schema.reload();
    database.getStorage().reload();

    schema.getClass("IndexInSubclassesTestBase").truncate();
    schema.getClass("IndexInSubclassesTestChild1").truncate();
    schema.getClass("IndexInSubclassesTestChild2").truncate();
  }

  @Test
  public void testIndexCrossReferencedDocuments() throws Exception {
    database.begin();

    for (int i = 0; i < 100000; i++) {
      final ODocument doc0 = new ODocument("IndexInSubclassesTestBase");
      doc0.field("name", "name" + i);
      doc0.save();

      final ODocument doc1 = new ODocument("IndexInSubclassesTestChild1");
      doc1.field("name", "name" + i);
      doc1.save();

      final ODocument doc2 = new ODocument("IndexInSubclassesTestChild2");
      doc2.field("name", "name" + i);
      doc2.save();
      if (i % 100 == 0) {
        database.commit();
      }
    }
    database.commit();

    long begin = System.currentTimeMillis();
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "select from IndexInSubclassesTestBase where name > 'name99995' and name < 'name99999' order by name ASC"));
    System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    Assert.assertTrue(result.size() == 9);
    String lastName = result.get(0).field("name");
    System.out.println(lastName);
    for (int i = 1; i < result.size(); i++) {
      ODocument current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) <= 0);
      lastName = currentName;
      System.out.println(currentName);
    }

    begin = System.currentTimeMillis();
    result = database.query(new OSQLSynchQuery<ODocument>(
        "select from IndexInSubclassesTestBase where name > 'name99995' and name < 'name99999' order by name DESC"));
    System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    Assert.assertTrue(result.size() == 9);
    lastName = result.get(0).field("name");
    System.out.println(lastName);
    for (int i = 1; i < result.size(); i++) {
      ODocument current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) >= 0);
      lastName = currentName;
      System.out.println(currentName);
    }
  }
}
