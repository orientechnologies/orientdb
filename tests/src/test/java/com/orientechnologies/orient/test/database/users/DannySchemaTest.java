package com.orientechnologies.orient.test.database.users;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class DannySchemaTest {
	private ODatabaseDocumentTx	db;
	private OClass							dependents;
	private OClass							master;

	@Test
	public void test1() {

		db = new ODatabaseDocumentTx("local:C:/work/dev/orientechnologies/orientdb/temp/danny/library/library");

		try {
			db.create();

			dependents = db.getMetadata().getSchema().createClass("Dependents");
			dependents.createProperty("type", OType.STRING);
			dependents.createProperty("dependents", OType.EMBEDDEDLIST, OType.STRING);

			master = db.getMetadata().getSchema().createClass("Master");
			master.createProperty("type", OType.STRING);
			master.createProperty("master", OType.STRING);

			db.getMetadata().getSchema().save();

			db.close();

			db.open("admin", "admin");
			dependents = db.getMetadata().getSchema().getClass("Dependents");
			master = db.getMetadata().getSchema().getClass("Master");
			db.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
