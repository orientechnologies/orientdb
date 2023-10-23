package com.orientechnologies.orient.core.sql.functions.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 07/04/15. */
public class OFunctionSqlTest extends BaseMemoryDatabase {

  @Test
  public void functionSqlWithParameters() {

    // ODatabaseRecordThreadLocal.instance().set(db);
    ODocument doc1 = new ODocument("Test");
    doc1.field("name", "Enrico");
    db.save(doc1);
    doc1.reset();
    doc1.setClassName("Test");
    doc1.field("name", "Luca");
    db.save(doc1);

    OFunction function = new OFunction();
    function.setName("test");
    function.setCode("select from Test where name = :name");
    function.setParameters(
        new ArrayList<String>() {
          {
            add("name");
          }
        });
    function.save();
    Object result = function.executeInContext(new OBasicCommandContext(), "Enrico");

    Assert.assertEquals(((OLegacyResultSet) result).size(), 1);
  }

  @Test
  public void functionSqlWithInnerFunctionJs() {

    // ODatabaseRecordThreadLocal.instance().set(db);
    ODocument doc1 = new ODocument("Test");
    doc1.field("name", "Enrico");
    db.save(doc1);
    doc1.reset();
    doc1.setClassName("Test");
    doc1.field("name", "Luca");
    db.save(doc1);

    OFunction function = new OFunction();
    function.setName("test");
    function.setCode("select name from Test where name = :name and hello(:name) = 'Hello Enrico'");
    function.setParameters(
        new ArrayList<String>() {
          {
            add("name");
          }
        });
    function.save();

    OFunction function1 = new OFunction();
    function1.setName("hello");
    function1.setLanguage("javascript");
    function1.setCode("return 'Hello ' + name");
    function1.setParameters(
        new ArrayList<String>() {
          {
            add("name");
          }
        });
    function1.save();
    Object result = function.executeInContext(new OBasicCommandContext(), "Enrico");

    Assert.assertEquals(((OLegacyResultSet) result).size(), 1);
  }
}
