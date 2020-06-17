package com.orientechnologies.orient.core.sql.parser;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Test;

public class OProfileStorageStatementTest {

  protected SimpleNode checkRightSyntax(String query) {
    SimpleNode result = checkSyntax(query, true);
    StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntax(builder.toString(), true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    OrientSql osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        //        System.out.println(query);
        //        if(result != null) {
        //          System.out.println("->");
        //          StringBuilder builer = new StringBuilder();
        //          result.toString(null, builer);
        //          System.out.println(builer.toString());
        //          System.out.println("............");
        //        }
        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        System.out.println(query);
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  @Test
  public void testParserSimple1() {
    SimpleNode stm = checkRightSyntax("profile storage on");
    assertTrue(stm instanceof OProfileStorageStatement);
  }

  @Test
  public void testParserSimple2() {
    SimpleNode stm = checkRightSyntax("profile storage off");
    assertTrue(stm instanceof OProfileStorageStatement);
  }

  @Test
  public void testParserSimpleUpper1() {
    SimpleNode stm = checkRightSyntax("PROFILE STORAGE ON");
    assertTrue(stm instanceof OProfileStorageStatement);
  }

  @Test
  public void testParserSimpleUpper2() {
    SimpleNode stm = checkRightSyntax("PROFILE STORAGE OFF");
    assertTrue(stm instanceof OProfileStorageStatement);
  }

  @Test
  public void testWrong() {
    checkWrongSyntax("PROFILE STORAGE");
    checkWrongSyntax("PROFILE x STORAGE OFF");
    checkWrongSyntax("PROFILE STORAGE x OFF");
    checkWrongSyntax("PROFILE STORAGE of");
    checkWrongSyntax("PROFILE STORAGE onn");
    checkWrongSyntax("PROFILE STORAGE off foo bar");
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
