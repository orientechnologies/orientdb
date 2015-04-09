package com.orientechnologies.orient.core.sql.functions.sql;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

/**
 * Created by Enrico Risa on 07/04/15.
 */
@Test
public class OFunctionSqlTest {

  @Test
  public void functionSqlWithParameters() {

    ODatabaseDocument db = new ODatabaseDocumentTx("memory:test");
    db.create();

    // ODatabaseRecordThreadLocal.INSTANCE.set(db);
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
    function.setParameters(new ArrayList<String>() {
      {
        add("name");
      }
    });
    function.save();
    Object result = function.executeInContext(new OBasicCommandContext(), "Enrico");

    System.out.println(result);

    Assert.assertEquals(((OResultSet) result).size(), 1);
    db.drop();
  }

  @Test
  public void functionSqlWithInnerFunctionJs() {

    ODatabaseDocument db = new ODatabaseDocumentTx("memory:test");
    db.create();

    // ODatabaseRecordThreadLocal.INSTANCE.set(db);
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
    function.setParameters(new ArrayList<String>() {
      {
        add("name");
      }
    });
    function.save();

    OFunction function1 = new OFunction();
    function1.setName("hello");
    function1.setLanguage("javascript");
    function1.setCode("return 'Hello ' + name");
    function1.setParameters(new ArrayList<String>() {
      {
        add("name");
      }
    });
    function1.save();
    Object result = function.executeInContext(new OBasicCommandContext(), "Enrico");

    System.out.println(result);

    Assert.assertEquals(((OResultSet) result).size(), 1);
    db.drop();
  }
}
