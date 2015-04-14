package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.metadata.schema.OSchemaProxyObject;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import javax.persistence.Id;
import java.io.Serializable;

/**
 * @author Wouter de Vaal
 */
@Test(groups = "schema")
public class SchemeGenerationTest  extends DocumentDBBaseTest {

    @Parameters(value = "url")
    public SchemeGenerationTest(@Optional String url) {
      super(url);
    }

    @Test
    public void checkSchemaGenerationSerializable() {
      database = new ODatabaseDocumentTx(url);
      database.open("admin", "admin");

      OObjectDatabaseTx oDatabase = new OObjectDatabaseTx(database);
      oDatabase.getEntityManager().registerEntityClass(Foo.class);
      oDatabase.getEntityManager().registerEntityClass(Bar.class);
      OSchemaProxyObject schema = oDatabase.getMetadata().getSchema();
      schema.generateSchema(Foo.class);
      schema.generateSchema(Bar.class);

      Assert.assertEquals(schema.getClass("Bar").getProperty("foo").getType(), OType.LINK);

      database.close();
    }

    private class Foo {
      @Id
      private Object id;

      public Object getId() {
        return id;
      }

      public void setId(Object id) {
        this.id = id;
      }
    }

    private class Bar {
      @Id
      private Object id;
      private Foo foo;

      public Object getId() {
        return id;
      }

      public void setId(Object id) {
        this.id = id;
      }

      public Foo getFoo() {
        return foo;
      }

      public void setFoo(Foo foo) {
        this.foo = foo;
      }
    }
}
