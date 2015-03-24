package com.orientechnologies.orient.core.metadata.schema;

import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;

public class TestMultiSuperClasses {
	private ODatabaseDocumentTx db;

	  @BeforeMethod
	  public void setUp() {
	    db = new ODatabaseDocumentTx("memory:" + TestMultiSuperClasses.class.getSimpleName());
	    if (db.exists()) {
	      db.open("admin", "admin");
	    } else
	      db.create();
	  }

	  @AfterMethod
	  public void after() {
	    db.close();
	  }
	  
	  @Test
	  public void testClassCreation()
	  {
		  final OSchema oSchema = db.getMetadata().getSchema();

		  OClass aClass = oSchema.createAbstractClass("javaA");
		  OClass bClass = oSchema.createAbstractClass("javaB");
		  OClass cClass = oSchema.createClass("javaC", aClass,bClass);
		  List<? extends OClass> superClasses;
		  //Run twice to be sure after schema reload
		  for(int i=0;i<2;i++)
		  {
			  superClasses = cClass.getSuperClasses();
			  assertTrue(superClasses.contains(aClass));
			  assertTrue(superClasses.contains(bClass));
			  assertTrue(cClass.isSubClassOf(aClass));
			  assertTrue(cClass.isSubClassOf(bClass));
			  assertTrue(aClass.isSuperClassOf(cClass));
			  assertTrue(bClass.isSuperClassOf(cClass));
			  oSchema.reload();
		  }
	  }
	  
	  @Test
	  public void testSql()
	  {
		  final OSchema oSchema = db.getMetadata().getSchema();

		  OClass aClass = oSchema.createAbstractClass("sqlA");
		  OClass bClass = oSchema.createAbstractClass("sqlB");
		  OClass cClass = oSchema.createClass("sqlC");
		  db.command(new OCommandSQL("alter class sqlC superclasses sqlA, sqlB")).execute();
		  oSchema.reload();
		  assertTrue(cClass.isSubClassOf(aClass));
		  assertTrue(cClass.isSubClassOf(bClass));
		  db.command(new OCommandSQL("alter class sqlC superclass sqlA")).execute();
		  oSchema.reload();
		  assertTrue(cClass.isSubClassOf(aClass));
		  assertFalse(cClass.isSubClassOf(bClass));
		  db.command(new OCommandSQL("alter class sqlC superclass +sqlB")).execute();
		  oSchema.reload();
		  assertTrue(cClass.isSubClassOf(aClass));
		  assertTrue(cClass.isSubClassOf(bClass));
		  db.command(new OCommandSQL("alter class sqlC superclass -sqlA")).execute();
		  oSchema.reload();
		  assertFalse(cClass.isSubClassOf(aClass));
		  assertTrue(cClass.isSubClassOf(bClass));
	  }
}
