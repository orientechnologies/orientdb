package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.testng.Assert.fail;

@Test
public class OBatchScriptTest {

  public void test(){
    checkRightSyntax("begin;select from foo; return bar");
    checkRightSyntax("begin;\nselect from foo;\n return bar");
    checkRightSyntax("begin;\nselect from foo;/*foo bar*/ return bar");
    checkRightSyntax("/*foo bar*/ begin;\nselect from foo;return bar/*foo bar*/ ");
  }

  protected List<OStatement> checkRightSyntax(String query) {
    return checkSyntax(query, true);
  }

  protected List<OStatement> checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected List<OStatement> checkSyntax(String query, boolean isCorrect) {
    OrientSql osql = getParserFor(query);
    try {
      List<OStatement> result = osql.parseScript();
      for(OStatement stm:result){
//        System.out.println(stm.toString()+";");
      }
      if (!isCorrect) {
        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }


  protected OrientSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    OrientSql osql = new OrientSql(is);
    return osql;
  }

}
