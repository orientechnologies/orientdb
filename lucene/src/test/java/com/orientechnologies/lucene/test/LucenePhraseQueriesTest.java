package com.orientechnologies.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 18/11/2016. */
public class LucenePhraseQueriesTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {

    OClass type = db.createVertexClass("Role");
    type.createProperty("name", OType.STRING);

    db.command(
            "create index Role.name on Role (name) FULLTEXT ENGINE LUCENE "
                + "METADATA{"
                + "\"name_index\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\","
                + "\"name_index_stopwords\": \"[]\","
                + "\"name_query\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\","
                + "\"name_query_stopwords\": \"[]\""
                //                + "\"name_query\":
                // \"org.apache.lucene.analysis.core.KeywordAnalyzer\""
                + "} ")
        .close();

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

    OResultSet vertexes = db.query("select from Role where name lucene ' \"Business Owner\" '  ");

    assertThat(vertexes.stream()).hasSize(1);

    vertexes = db.query("select from Role where name lucene ' \"Owner of Business\" '  ");

    assertThat(vertexes.stream()).hasSize(0);

    vertexes = db.query("select from Role where name lucene ' \"System Owner\" '  ");

    assertThat(vertexes.stream()).hasSize(0);

    vertexes = db.query("select from Role where name lucene ' \"System SME\"~1 '  ");

    assertThat(vertexes.stream()).hasSize(2);

    vertexes = db.query("select from Role where name lucene ' \"System Business\"~1 '  ");

    assertThat(vertexes.stream()).hasSize(2);

    vertexes = db.query("select from Role where name lucene ' /[mb]oat/ '  ");

    assertThat(vertexes.stream()).hasSize(2);
  }

  @Test
  public void testComplexPhraseQueries() throws Exception {

    OResultSet vertexes = db.query("select from Role where name lucene ?", "\"System SME\"~1");

    assertThat(vertexes.stream()).allMatch(v -> v.<String>getProperty("name").contains("SME"));

    vertexes = db.query("select from Role where name lucene ? ", "\"SME System\"~1");

    assertThat(vertexes.stream()).isEmpty();

    vertexes = db.query("select from Role where name lucene ? ", "\"Owner Of Business\"");
    vertexes.stream().forEach(v -> System.out.println("v = " + v.getProperty("name")));

    assertThat(vertexes.stream()).isEmpty();

    vertexes = db.query("select from Role where name lucene ? ", "\"System Business SME\"");

    assertThat(vertexes.stream())
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business SME"));

    vertexes = db.query("select from Role where name lucene ? ", "\"System Owner\"~1 -IT");
    assertThat(vertexes.stream())
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));

    vertexes = db.query("select from Role where name lucene ? ", "+System +Own*~0.0 -IT");
    assertThat(vertexes.stream())
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));

    vertexes = db.query("select from Role where name lucene ? ", "\"System Owner\"~1 -Business");
    assertThat(vertexes.stream())
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System IT Owner"));
  }
}
