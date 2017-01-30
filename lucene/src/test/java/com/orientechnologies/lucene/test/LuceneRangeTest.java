package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.apache.lucene.document.DateTools;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 13/12/2016.
 */
public class LuceneRangeTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    OSchemaProxy schema = db.getMetadata().getSchema();

    OClass cls = schema.createClass("Person");
    cls.createProperty("name", OType.STRING);
    cls.createProperty("surname", OType.STRING);
    cls.createProperty("date", OType.DATETIME);
    cls.createProperty("age", OType.INTEGER);

    List<String> names = Arrays.asList("John", "Robert", "Jane", "andrew", "Scott", "luke", "Enriquez", "Luis", "Gabriel", "Sara");
    for (int i = 0; i < 10; i++) {
      db.save(new ODocument("Person")
          .field("name", names.get(i))
          .field("surname", "Reese")
          //from today back one day a time
          .field("date", System.currentTimeMillis() - (i * 3600 * 24 * 1000))
          .field("age", i));
    }

  }

  @Test
  public void shouldUseRangeQueryOnSingleIntegerField() throws Exception {

    db.command(new OCommandSQL("create index Person.age on Person(age) FULLTEXT ENGINE LUCENE")).execute();

    assertThat(db.getMetadata().getIndexManager().getIndex("Person.age").getSize()).isEqualTo(10);

    //range
    Collection<ODocument> results = db.command(
        new OCommandSQL("SELECT FROM Person WHERE age LUCENE 'age:[5 TO 6]'"))
        .execute();

    assertThat(results).hasSize(2);

    //single value
    results = db.command(
        new OCommandSQL("SELECT FROM Person WHERE age LUCENE 'age:5'"))
        .execute();

    assertThat(results).hasSize(1);
  }

  @Test
  public void shouldUseRangeQueryOnSingleDateField() throws Exception {

    db.commit();
    db.command(new OCommandSQL("create index Person.date on Person(date) FULLTEXT ENGINE LUCENE")).execute();
    db.commit();

    assertThat(db.getMetadata().getIndexManager().getIndex("Person.date").getSize()).isEqualTo(10);

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo = DateTools.timeToString(System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    //range
    Collection<ODocument> results = db.command(
        new OCommandSQL("SELECT FROM Person WHERE date LUCENE 'date:[" + fiveDaysAgo + " TO " + today + "]'"))
        .execute();

    assertThat(results).hasSize(5);

  }

  @Test
  public void shouldUseRangeQueryMultipleField() throws Exception {

    db.command(new OCommandSQL("create index Person.composite on Person(name,surname,date,age) FULLTEXT ENGINE LUCENE")).execute();

    assertThat(db.getMetadata().getIndexManager().getIndex("Person.composite").getSize()).isEqualTo(10);

    db.commit();

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo = DateTools.timeToString(System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    //anme and age range
    Collection<ODocument> results = db.command(
        new OCommandSQL("SELECT * FROM Person WHERE [name,surname,date,age] LUCENE 'name:luke  age:[5 TO 6]'"))
        .execute();

    assertThat(results).hasSize(2);

    //date range
    results = db.command(
        new OCommandSQL("SELECT FROM Person WHERE [name,surname,date,age] LUCENE 'date:[" + fiveDaysAgo + " TO " + today + "]'"))
        .execute();

    assertThat(results).hasSize(5);

    //age and date range with MUST
    results = db.command(
        new OCommandSQL(
            "SELECT FROM Person WHERE [name,surname,date,age] LUCENE '+age:[4 TO 7]  +date:[" + fiveDaysAgo + " TO " + today
                + "]'"))
        .execute();

    assertThat(results).hasSize(2);

  }

  @Test
  public void shouldFetchOnlyFromACluster() throws Exception {

    db.command(new OCommandSQL("create index Person.name on Person(name) FULLTEXT ENGINE LUCENE")).execute();

    assertThat(db.getMetadata().getIndexManager().getIndex("Person.name").getSize()).isEqualTo(10);

    int cluster = db.getMetadata().getSchema().getClass("Person").getClusterIds()[1];
    db.commit();

//age and date range with MUST
    Collection<ODocument> results = db.command(
        new OCommandSQL(
            "SELECT FROM Person WHERE name LUCENE '+_CLUSTER:" + cluster +"'"))
        .execute();

    assertThat(results).hasSize(2);

  }
}
