package com.orientechnologies.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 18/11/2016. */
public class LucenePhraseQueriesTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {

    OClass type = db.createVertexClass("Role");
    type.createProperty("name", OType.STRING);

    db.command(
            new OCommandSQL(
                "create index Role.name on Role (name) FULLTEXT ENGINE LUCENE "
                    + "METADATA{"
                    + "\"name_index\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\","
                    + "\"name_index_stopwords\": \"[]\","
                    + "\"name_query\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\","
                    + "\"name_query_stopwords\": \"[]\""
                    //                + "\"name_query\":
                    // \"org.apache.lucene.analysis.core.KeywordAnalyzer\""
                    + "} "))
        .execute();

    OVertex role = db.newVertex("Role");
    role.setProperty("name", "System IT Owner");
    db.save(role);

    role = db.newVertex("Role");
    role.setProperty("name", "System Business Owner");
    db.save(role);

    role = db.newVertex("Role");
    role.setProperty("name", "System Business SME");
    db.save(role);

    role = db.newVertex("Role");
    role.setProperty("name", "System Technical SME");
    db.save(role);

    role = db.newVertex("Role");
    role.setProperty("name", "System");
    db.save(role);

    role = db.newVertex("Role");
    role.setProperty("name", "boat");
    db.save(role);

    role = db.newVertex("Role");
    role.setProperty("name", "moat");
    db.save(role);
  }

  @Test
  public void testPhraseQueries() throws Exception {

    OLegacyResultSet vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ' \"Business Owner\" '  "))
            .execute();

    assertThat(vertexes).hasSize(1);

    vertexes =
        db.command(
                new OCommandSQL("select from Role where name lucene ' \"Owner of Business\" '  "))
            .execute();

    assertThat(vertexes).hasSize(0);

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ' \"System Owner\" '  "))
            .execute();

    assertThat(vertexes).hasSize(0);

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ' \"System SME\"~1 '  "))
            .execute();

    assertThat(vertexes).hasSize(2);

    vertexes =
        db.command(
                new OCommandSQL("select from Role where name lucene ' \"System Business\"~1 '  "))
            .execute();

    assertThat(vertexes).hasSize(2);

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ' /[mb]oat/ '  ")).execute();

    assertThat(vertexes).hasSize(2);
  }

  @Test
  public void testComplexPhraseQueries() throws Exception {

    Iterable<ODocument> vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ?"))
            .execute("\"System SME\"~1");

    assertThat(vertexes).allMatch(v -> v.<String>getProperty("name").contains("SME"));

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ? "))
            .execute("\"SME System\"~1");

    assertThat(vertexes).isEmpty();

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ? "))
            .execute("\"Owner Of Business\"");
    vertexes.forEach(v -> System.out.println("v = " + v.getProperty("name")));

    assertThat(vertexes).isEmpty();

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ? "))
            .execute("\"System Business SME\"");

    assertThat(vertexes)
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business SME"));

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ? "))
            .execute("\"System Owner\"~1 -IT");
    assertThat(vertexes)
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ? "))
            .execute("+System +Own*~0.0 -IT");
    assertThat(vertexes)
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));

    vertexes =
        db.command(new OCommandSQL("select from Role where name lucene ? "))
            .execute("\"System Owner\"~1 -Business");
    assertThat(vertexes)
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System IT Owner"));
  }
}
