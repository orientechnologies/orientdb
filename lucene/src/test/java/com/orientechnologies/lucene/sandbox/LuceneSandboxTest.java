package com.orientechnologies.lucene.sandbox;

import com.orientechnologies.lucene.tests.OLuceneBaseTest;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 02/01/2017. */
public class LuceneSandboxTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    db.command("CREATE CLASS CDR");
    db.command("CREATE PROPERTY  CDR.filename STRING");
    db.command(
        "INSERT into cdr(filename) values('MDCA10MCR201612291808.276388.eno.RRC.20161229183002.PROD_R4.eno.data') ");
    db.command(
        "INSERT into cdr(filename) values('MDCA20MCR201612291911.277904.eno.RRC.20161229193002.PROD_R4.eno.data') ");
  }

  @Test
  public void shouldFetchOneDocumentWithExactMatchOnLuceneIndexStandardAnalyzer() throws Exception {
    db.command("CREATE INDEX cdr.filename ON cdr(filename) FULLTEXT ENGINE LUCENE ");
    // partial match
    OResultSet res =
        db.query("select from cdr WHERE filename LUCENE ' RRC.20161229193002.PROD_R4.eno.data '");

    Assertions.assertThat(res).hasSize(2);
    res.close();
    // exact match
    res =
        db.query(
            "select from cdr WHERE filename LUCENE ' \"MDCA20MCR201612291911.277904.eno.RRC.20161229193002.PROD_R4.eno.data\" '");

    Assertions.assertThat(res).hasSize(1);
    res.close();
    // wildcard
    res = db.query("select from cdr WHERE filename LUCENE ' MDCA* '");

    Assertions.assertThat(res).hasSize(2);
    res.close();
  }

  @Test
  public void shouldFetchOneDocumentWithExactMatchOnLuceneIndexKeyWordAnalyzer() throws Exception {

    db.command(
        "CREATE INDEX cdr.filename ON cdr(filename) FULLTEXT ENGINE LUCENE metadata { 'allowLeadingWildcard': true}");

    // partial match
    OResultSet res =
        db.query(
            "select from cdr WHERE SEARCH_CLASS( ' RRC.20161229193002.PROD_R4.eno.data ') = true");

    Assertions.assertThat(res).hasSize(2);
    res.close();
    // exact match
    res =
        db.query(
            "select from cdr WHERE SEARCH_CLASS( ' \"MDCA20MCR201612291911.277904.eno.RRC.20161229193002.PROD_R4.eno.data\" ') = true");

    Assertions.assertThat(res).hasSize(1);
    res.close();
    // wildcard
    res = db.query("select from cdr WHERE SEARCH_CLASS(' MDCA* ')= true");
    res.close();
    // leadind wildcard
    res = db.query("select from cdr WHERE SEARCH_CLASS(' *20MCR2016122* ') =true");

    Assertions.assertThat(res).hasSize(1);
    res.close();
  }

  @Test
  public void testHierarchy() throws Exception {

    db.command("CREATE Class Father EXTENDS V");
    db.command("CREATE PROPERTY Father.text STRING");

    db.command("CREATE INDEX Father.text ON Father(text) FULLTEXT ENGINE LUCENE ");

    db.command("CREATE Class Son EXTENDS Father");
    db.command("CREATE PROPERTY Son.textOfSon STRING");

    db.command("CREATE INDEX Son.textOfSon ON Son(textOfSon) FULLTEXT ENGINE LUCENE ");
    OClass father = db.getMetadata().getSchema().getClass("Father");
  }

  @Test
  public void documentSertest() throws Exception {

    Document doc = new Document();
    doc.add(new StringField("text", "yabba dabba", Field.Store.YES));

    SimpleTextCodec codec = new SimpleTextCodec();
  }

  @Test
  public void charset() throws Exception {

    String element = ";";
    int x = element.charAt(0);
    System.out.println("x=" + x);
  }
}
