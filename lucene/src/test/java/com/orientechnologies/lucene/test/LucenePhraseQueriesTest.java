package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Created by frank on 18/11/2016.
 */
public class LucenePhraseQueriesTest extends BaseLuceneTest {

  private OrientGraph graph;

  @Before
  public void setUp() throws Exception {

    graph = new OrientGraph(db, false);
    OrientVertexType type = graph.createVertexType("Role");
    type.createProperty("name", OType.STRING);
    graph.commit();

    db.command(new OCommandSQL("create index Role.name on Role (name) FULLTEXT ENGINE LUCENE "
        + "METADATA{"
        + "\"name_index\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\","
        + "\"name_index_stopwords\": \"[]\","
        + "\"name_query\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\","
        + "\"name_query_stopwords\": \"[]\""
        //                + "\"name_query\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\""
        + "} "
    )).execute();

    graph.addVertex("class:Role").setProperty("name", "System IT Owner");
    graph.addVertex("class:Role").setProperty("name", "System Business Owner");
    graph.addVertex("class:Role").setProperty("name", "System Business SME");
    graph.addVertex("class:Role").setProperty("name", "System Technical SME");
    graph.addVertex("class:Role").setProperty("name", "System");
    graph.addVertex("class:Role").setProperty("name", "boat");
    graph.addVertex("class:Role").setProperty("name", "moat");

    graph.commit();

  }

  @Test
  public void testPhraseQueries() throws Exception {

    Iterable<OrientVertex> vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ' \"Business Owner\" '  ")).execute();

    assertThat(vertexes).hasSize(1);

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ' \"Owner of Business\" '  ")).execute();

    assertThat(vertexes).hasSize(0);

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ' \"System Owner\" '  ")).execute();

    assertThat(vertexes).hasSize(0);

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ' \"System SME\"~1 '  ")).execute();

    assertThat(vertexes).hasSize(2);

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ' \"System Business\"~1 '  ")).execute();

    assertThat(vertexes).hasSize(2);

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ' /[mb]oat/ '  ")).execute();

    assertThat(vertexes).hasSize(2);

  }

  @Test
  public void testComplexPhraseQueries() throws Exception {

    Iterable<OrientVertex> vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ?"))
        .execute("\"System SME\"~1");

    assertThat(vertexes).allMatch(v -> v.<String>getProperty("name").contains("SME"));

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ? "))
        .execute("\"SME System\"~1");

    assertThat(vertexes).isEmpty();

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ? "))
        .execute("\"Owner Of Business\"");
    vertexes.forEach(v -> System.out.println("v = " + v.getProperty("name")));

    assertThat(vertexes).isEmpty();

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ? "))
        .execute("\"System Business SME\"");

    assertThat(vertexes).hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business SME"));

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ? "))
        .execute("\"System Owner\"~1 -business");

    assertThat(vertexes).hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System IT Owner"));

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ? "))
        .execute("\"System Owner\"~1 -IT");
    assertThat(vertexes).hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));

    vertexes = graph.command(
        new OCommandSQL("select from Role where name lucene ? "))
        .execute("+System +Own*~0.0 -IT");
    assertThat(vertexes).hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));
  }
}
