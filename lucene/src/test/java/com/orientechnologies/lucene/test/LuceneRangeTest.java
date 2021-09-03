package com.orientechnologies.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.document.DateTools;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 13/12/2016. */
public class LuceneRangeTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    OSchema schema = db.getMetadata().getSchema();

    OClass cls = schema.createClass("Person");
    cls.createProperty("name", OType.STRING);
    cls.createProperty("surname", OType.STRING);
    cls.createProperty("date", OType.DATETIME);
    cls.createProperty("age", OType.INTEGER);

    List<String> names =
        Arrays.asList(
            "John",
            "Robert",
            "Jane",
            "andrew",
            "Scott",
            "luke",
            "Enriquez",
            "Luis",
            "Gabriel",
            "Sara");
    for (int i = 0; i < 10; i++) {
      db.save(
          new ODocument("Person")
              .field("name", names.get(i))
              .field("surname", "Reese")
              // from today back one day a time
              .field("date", System.currentTimeMillis() - (i * 3600 * 24 * 1000))
              .field("age", i));
    }
  }

  @Test
  public void shouldUseRangeQueryOnSingleIntegerField() {
    //noinspection deprecation
    db.command(new OCommandSQL("create index Person.age on Person(age) FULLTEXT ENGINE LUCENE"))
        .execute();

    assertThat(
            db.getMetadata()
                .getIndexManagerInternal()
                .getIndex(db, "Person.age")
                .getInternal()
                .size())
        .isEqualTo(10);

    // range
    @SuppressWarnings("deprecation")
    Collection<ODocument> results =
        db.command(new OCommandSQL("SELECT FROM Person WHERE age LUCENE 'age:[5 TO 6]'")).execute();

    assertThat(results).hasSize(2);

    // single value
    //noinspection deprecation
    results = db.command(new OCommandSQL("SELECT FROM Person WHERE age LUCENE 'age:5'")).execute();

    assertThat(results).hasSize(1);
  }

  @Test
  public void shouldUseRangeQueryOnSingleDateField() {

    db.commit();
    //noinspection deprecation
    db.command(new OCommandSQL("create index Person.date on Person(date) FULLTEXT ENGINE LUCENE"))
        .execute();
    db.commit();

    assertThat(
            db.getMetadata()
                .getIndexManagerInternal()
                .getIndex(db, "Person.date")
                .getInternal()
                .size())
        .isEqualTo(10);

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo =
        DateTools.timeToString(
            System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    // range
    @SuppressWarnings("deprecation")
    Collection<ODocument> results =
        db.command(
                new OCommandSQL(
                    "SELECT FROM Person WHERE date LUCENE 'date:["
                        + fiveDaysAgo
                        + " TO "
                        + today
                        + "]'"))
            .execute();

    assertThat(results).hasSize(5);
  }

  @Test
  public void shouldUseRangeQueryMultipleField() {
    //noinspection deprecation
    db.command(
            new OCommandSQL(
                "create index Person.composite on Person(name,surname,date,age) FULLTEXT ENGINE LUCENE"))
        .execute();

    assertThat(
            db.getMetadata()
                .getIndexManagerInternal()
                .getIndex(db, "Person.composite")
                .getInternal()
                .size())
        .isEqualTo(10);

    db.commit();

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo =
        DateTools.timeToString(
            System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    // name and age range
    @SuppressWarnings("deprecation")
    Collection<ODocument> results =
        db.command(
                new OCommandSQL(
                    "SELECT * FROM Person WHERE [name,surname,date,age] LUCENE 'age:[5 TO 6] name:robert  '"))
            .execute();

    assertThat(results).hasSize(3);

    // date range
    //noinspection deprecation
    results =
        db.command(
                new OCommandSQL(
                    "SELECT FROM Person WHERE [name,surname,date,age] LUCENE 'date:["
                        + fiveDaysAgo
                        + " TO "
                        + today
                        + "]'"))
            .execute();

    assertThat(results).hasSize(5);

    // age and date range with MUST
    //noinspection deprecation
    results =
        db.command(
                new OCommandSQL(
                    "SELECT FROM Person WHERE [name,surname,date,age] LUCENE '+age:[4 TO 7]  +date:["
                        + fiveDaysAgo
                        + " TO "
                        + today
                        + "]'"))
            .execute();

    assertThat(results).hasSize(2);
  }

  @Test
  public void shouldUseRangeQueryMultipleFieldWithDirectIndexAccess() {
    //noinspection deprecation
    db.command(
            new OCommandSQL(
                "create index Person.composite on Person(name,surname,date,age) FULLTEXT ENGINE LUCENE"))
        .execute();

    assertThat(
            db.getMetadata()
                .getIndexManagerInternal()
                .getIndex(db, "Person.composite")
                .getInternal()
                .size())
        .isEqualTo(10);

    db.commit();

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo =
        DateTools.timeToString(
            System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    // name and age range
    final OIndex index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "Person.composite");
    try (Stream<ORID> stream = index.getInternal().getRids("name:luke  age:[5 TO 6]")) {
      assertThat(stream.count()).isEqualTo(2);
    }

    // date range
    try (Stream<ORID> stream =
        index.getInternal().getRids("date:[" + fiveDaysAgo + " TO " + today + "]")) {
      assertThat(stream.count()).isEqualTo(5);
    }

    // age and date range with MUST
    try (Stream<ORID> stream =
        index
            .getInternal()
            .getRids("+age:[4 TO 7]  +date:[" + fiveDaysAgo + " TO " + today + "]")) {
      assertThat(stream.count()).isEqualTo(2);
    }
  }

  @Test
  public void shouldFetchOnlyFromACluster() {
    //noinspection deprecation
    db.command(new OCommandSQL("create index Person.name on Person(name) FULLTEXT ENGINE LUCENE"))
        .execute();

    assertThat(
            db.getMetadata()
                .getIndexManagerInternal()
                .getIndex(db, "Person.name")
                .getInternal()
                .size())
        .isEqualTo(10);

    int cluster = db.getMetadata().getSchema().getClass("Person").getClusterIds()[1];
    db.commit();

    @SuppressWarnings("deprecation")
    Collection<ODocument> results =
        db.command(
                new OCommandSQL("SELECT FROM Person WHERE name LUCENE '+_CLUSTER:" + cluster + "'"))
            .execute();

    assertThat(results).hasSize(2);
  }
}
