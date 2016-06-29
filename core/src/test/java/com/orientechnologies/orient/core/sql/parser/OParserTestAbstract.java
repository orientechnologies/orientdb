package com.orientechnologies.orient.core.sql.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.fail;

public abstract class OParserTestAbstract {

  protected SimpleNode checkRightSyntax(String query) {
    SimpleNode result = checkSyntax(query, true);
    StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntax(builder.toString(), true);
    //    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    OrientSql osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        System.out.println(query);
        if (result != null) {
          System.out.println("->");
          StringBuilder builer = new StringBuilder();
          result.toString(null, builer);
          System.out.println(builer.toString());
          System.out.println("............");
        }

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
