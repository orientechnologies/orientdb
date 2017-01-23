package com.orientechnologies.lucene.sandbox;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;

/**
 * Created by frank on 02/01/2017.
 */
public class LuceneSandboxTest extends BaseLuceneTest {

  @Test
  public void shouldFetchOneDocumentWithExactMatchOnLuceneIndexStandardAnalyzer() throws Exception {

    db.command(new OCommandSQL("CREATE CLASS CDR")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY  CDR.filename STRING")).execute();
    db.command(new OCommandSQL(
        "CREATE INDEX cdr.filename ON cdr(filename) FULLTEXT ENGINE LUCENE ")).execute();
    db.command(new OCommandSQL(
        "INSERT into cdr(filename) values('MDCA10MCR201612291808.276388.eno.RRC.20161229183002.PROD_R4.eno.data') ")).execute();
    db.command(new OCommandSQL(
        "INSERT into cdr(filename) values('MDCA20MCR201612291911.277904.eno.RRC.20161229193002.PROD_R4.eno.data') ")).execute();

    //partial match
    List<ODocument> res = db.query(new OSQLSynchQuery<ODocument>(
        "select from cdr WHERE filename LUCENE ' RRC.20161229193002.PROD_R4.eno.data '"));

    Assertions.assertThat(res).hasSize(2);

    //exact match
    res = db.query(new OSQLSynchQuery<ODocument>(
        "select from cdr WHERE filename LUCENE ' \"MDCA20MCR201612291911.277904.eno.RRC.20161229193002.PROD_R4.eno.data\" '"));

    Assertions.assertThat(res).hasSize(1);

    //wildcard
    res = db.query(new OSQLSynchQuery<ODocument>(
        "select from cdr WHERE filename LUCENE ' MDCA* '"));

    Assertions.assertThat(res).hasSize(2);
  }

  @Test
  public void shouldFetchOneDocumentWithExactMatchOnLuceneIndexKeyWordAnalyzer() throws Exception {

    db.command(new OCommandSQL("CREATE CLASS CDR")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY CDR.filename STRING")).execute();
    db.command(new OCommandSQL(
        "CREATE INDEX cdr.filename ON cdr(filename) FULLTEXT ENGINE LUCENE metadata { 'allowLeadingWildcard': true}")).execute();
    db.command(new OCommandSQL(
        "INSERT into cdr(filename) values('MDCA10MCR201612291808.276388.eno.RRC.20161229183002.PROD_R4.eno.data') ")).execute();
    db.command(new OCommandSQL(
        "INSERT into cdr(filename) values('MDCA20MCR201612291911.277904.eno.RRC.20161229193002.PROD_R4.eno.data') ")).execute();

    //partial match
    List<ODocument> res = db.query(new OSQLSynchQuery<ODocument>(
        "select from cdr WHERE filename LUCENE ' RRC.20161229193002.PROD_R4.eno.data '"));

    Assertions.assertThat(res).hasSize(2);

    //exact match
    res = db.query(new OSQLSynchQuery<ODocument>(
        "select from cdr WHERE filename LUCENE ' \"MDCA20MCR201612291911.277904.eno.RRC.20161229193002.PROD_R4.eno.data\" '"));

    Assertions.assertThat(res).hasSize(1);

    //wildcard
    res = db.query(new OSQLSynchQuery<ODocument>(
        "select from cdr WHERE filename LUCENE ' MDCA* '"));

    //leadind wildcard
    res = db.query(new OSQLSynchQuery<ODocument>(
        "select from cdr WHERE filename LUCENE ' *20MCR2016122* '"));

    Assertions.assertThat(res).hasSize(1);
  }

  @Test
  public void testTokenStream() throws Exception {

    String text = "MDMA10MCR201612291750.275868.eno.RRC.20161229180001.PROD_R4.eno.data";
    Analyzer analyzer = new WhitespaceAnalyzer();

    TokenStream stream = analyzer.tokenStream(null, new StringReader(text));
    stream.reset();
    while (stream.incrementToken()) {
      System.out.println(stream.getAttribute(CharTermAttribute.class).toString());
    }
  }
}
