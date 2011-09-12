package com.orientechnologies.orient.jdbc;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class OrientJdbcStatementDMLtest extends OrientJdbcBaseTest {

	@Test
	public void shouldCreateANewRow() throws Exception {

		assertFalse(conn.isClosed());
		Date date = new Date(System.currentTimeMillis());

		Statement stmt = conn.createStatement();
		int updated = stmt.executeUpdate("INSERT into Item (stringKey, intKey, text, length, date) values ('100','100','dummy text','10','" + date.toString() + "')");

		assertEquals(1, updated);

		stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '100' ");
		rs.next();
		assertEquals(100, rs.getInt("intKey"));
		assertEquals("100", rs.getString("stringKey"));
		assertEquals(date.toString(), rs.getDate("date").toString());

	}

	@Test
	public void shouldUpdateARow() throws Exception {

		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();
		int updated = stmt.executeUpdate("UPDATE Item set text = 'UPDATED'  WHERE intKey = '10'");

		assertFalse(stmt.getMoreResults());
		assertEquals(1, updated);

		stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
		rs.next();
		assertEquals("UPDATED", rs.getString("text"));

	}

	@Test
	public void shouldDeleteArow() throws Exception {

		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();
		int updated = stmt.executeUpdate("DELETE FROM Item WHERE intKey = '10'");

		assertFalse(stmt.getMoreResults());
		assertEquals(1, updated);

		stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
		assertFalse(rs.next());

	}

	@Test
	public void shoulCreateClassWithProperties() throws IOException, SQLException {

		Statement stmt = conn.createStatement();

		stmt.executeUpdate("CREATE CLASS Account ");
		stmt.executeUpdate("CREATE PROPERTY Account.id INTEGER ");
		stmt.executeUpdate("CREATE PROPERTY Account.birthDate DATE ");
		stmt.executeUpdate("CREATE PROPERTY Account.binary BINARY ");

		// double value test pattern?
		ODatabaseDocumentTx database = conn.getDatabase();
		assertThat(database.getClusterIdByName("account"), notNullValue());
		OClass account = database.getMetadata().getSchema().getClass("Account");
		assertThat(account, notNullValue());
		assertThat(account.getProperty("id").getType(), equalTo(OType.INTEGER));
		assertThat(account.getProperty("birthDate").getType(), equalTo(OType.DATE));
		assertThat(account.getProperty("binary").getType(), equalTo(OType.BINARY));

		// database.getMetadata().getSchema().createClass("Company", account);
		//
		// OClass profile =
		// database.getMetadata().getSchema().createClass("Profile",
		// database.addCluster("profile", OStorage.CLUSTER_TYPE.PHYSICAL));
		// profile.createProperty("nick",
		// OType.STRING).setMin("3").setMax("30").createIndex(INDEX_TYPE.UNIQUE);
		// profile.createProperty("name",
		// OType.STRING).setMin("3").setMax("30").createIndex(INDEX_TYPE.NOTUNIQUE);
		// profile.createProperty("surname",
		// OType.STRING).setMin("3").setMax("30");
		// profile.createProperty("registeredOn",
		// OType.DATETIME).setMin("2010-01-01 00:00:00");
		// profile.createProperty("lastAccessOn",
		// OType.DATETIME).setMin("2010-01-01 00:00:00");
		// profile.createProperty("photo", OType.TRANSIENT);
		//
		// OClass whiz = database.getMetadata().getSchema().createClass("Whiz");
		// whiz.createProperty("id", OType.INTEGER);
		// whiz.createProperty("account", OType.LINK, profile);
		// whiz.createProperty("date", OType.DATE).setMin("2010-01-01");
		// whiz.createProperty("text",
		// OType.STRING).setMandatory(true).setMin("1").setMax("140").createIndex(INDEX_TYPE.FULLTEXT);
		// whiz.createProperty("replyTo", OType.LINK, profile);
		//
		// database.close();
	}
}
