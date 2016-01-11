package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.testng.Assert.fail;

@Test
public class OFindReferencesTest {

  protected SimpleNode checkRightSyntax(String query) {
    SimpleNode result = checkSyntax(query, true);
    return checkSyntax(result.toString(), true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    OrientSql osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        fail();
      }
//      System.out.println(query);
//      System.out.println("->");
//      System.out.println(result.toString());
//      System.out.println("............");
      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  public void testSyntax(){
    checkRightSyntax("FIND REFERENCES #12:0");
    checkRightSyntax("find references #12:0");
    checkRightSyntax("FIND REFERENCES #12:0 [Person]");
    checkRightSyntax("FIND REFERENCES #12:0 [Person, Animal]");
    checkRightSyntax("FIND REFERENCES #12:0 [Person, cluster:animal]");
    checkRightSyntax("FIND REFERENCES (select from foo where name = ?)");
    checkRightSyntax("FIND REFERENCES (select from foo where name = ?) [Person, cluster:animal]");
    checkWrongSyntax("FIND REFERENCES ");
    checkWrongSyntax("FIND REFERENCES #12:0 #12:1");
    checkWrongSyntax("FIND REFERENCES #12:0, #12:1");
    checkWrongSyntax("FIND REFERENCES [#12:0, #12:1]");
    checkWrongSyntax("FIND REFERENCES foo");
  }

  private void printTree(String s) {
    OrientSql osql = getParserFor(s);
    try {
      SimpleNode n = osql.parse();

    } catch (ParseException e) {
      e.printStackTrace();
    }

  }

  protected OrientSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    OrientSql osql = new OrientSql(is);
    return osql;
  }
}
