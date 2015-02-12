package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.OConstants;
import org.junit.Before;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class OrientJdbcDatabaseMetaDataTest extends OrientJdbcBaseTest {

  private DatabaseMetaData metaData;

  @Before
  public void setup() throws SQLException {
    metaData = conn.getMetaData();

  }

  @Test
  public void verifyDriverAndDatabaseVersions() throws SQLException {

    assertEquals("memory:test", metaData.getURL());
    assertEquals("admin", metaData.getUserName());
    assertEquals("OrientDB", metaData.getDatabaseProductName());
    assertEquals(OConstants.ORIENT_VERSION, metaData.getDatabaseProductVersion());
    assertEquals(2, metaData.getDatabaseMajorVersion());
    assertEquals(1, metaData.getDatabaseMinorVersion());

    assertEquals("OrientDB JDBC Driver", metaData.getDriverName());
    assertEquals("OrientDB 2.1 JDBC Driver", metaData.getDriverVersion());
    assertEquals(2, metaData.getDriverMajorVersion());
    assertEquals(1, metaData.getDriverMinorVersion());

  }

  @Test
  public void shouldRetrievePrimaryKeysMetadata() throws SQLException {

    ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, "Item");
    assertTrue(primaryKeys.next());
    assertEquals("intKey", primaryKeys.getString(4));
    assertEquals("Item.intKey", primaryKeys.getString(6));
    assertEquals(1, primaryKeys.getInt(5));

    assertTrue(primaryKeys.next());
    assertEquals("stringKey", primaryKeys.getString("COLUMN_NAME"));
    assertEquals("Item.stringKey", primaryKeys.getString("PK_NAME"));
    assertEquals(1, primaryKeys.getInt("KEY_SEQ"));

  }

  @Test
  public void shouldRetrieveTableTypes() throws SQLException {

    ResultSet tableTypes = metaData.getTableTypes();
    assertTrue(tableTypes.next());
    assertEquals("TABLE", tableTypes.getString(1));
    assertTrue(tableTypes.next());
    assertEquals("SYSTEM TABLE", tableTypes.getString(1));

    assertFalse(tableTypes.next());

  }

  @Test
  public void shouldRetrieveKeywords() throws SQLException {

    final String keywordsStr = metaData.getSQLKeywords();
    assertNotNull(keywordsStr);
    assertThat(Arrays.asList(keywordsStr.toUpperCase().split(",\\s*")), hasItem("TRAVERSE"));
  }

  @Test
  public void getFields() throws SQLException {
    ResultSet rs = conn.createStatement().executeQuery("select from OUser");
    if (rs != null) {
      ResultSetMetaData metaData = rs.getMetaData();
      int cc = metaData.getColumnCount();
      Set<String> colset = new HashSet<String>();
      List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>(cc);
      for (int i = 1; i <= cc; i++) {
        String name = metaData.getColumnLabel(i);
        if (colset.contains(name))
          continue;
        colset.add(name);
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("name", name);

        try {
          String catalog = metaData.getCatalogName(i);
          String schema = metaData.getSchemaName(i);
          String table = metaData.getTableName(i);
          ResultSet rsmc = conn.getMetaData().getColumns(catalog, schema, table, name);
          while (rsmc.next()) {
            field.put("description", rsmc.getString("REMARKS"));
            break;
          }
        } catch (SQLException se) {
          se.printStackTrace();
        }
        columns.add(field);
      }

      for (Map<String, Object> c : columns) {
        System.out.println(c);
      }
    }
  }

  @Test
  public void getAllTables() throws SQLException {
    ResultSet rs = this.metaData.getTables(null, null, null, null);
    int tableCount = 0;

    while (rs.next()) {
      tableCount = tableCount + 1;
    }
    assertTrue(tableCount > 1);
  }

  @Test
  public void getAllTablesFilteredByAllTypes() throws SQLException {
    ResultSet rs = this.metaData.getTableTypes();
    List<String> tableTypes = new ArrayList<String>(2);
    while (rs.next()){
      tableTypes.add(rs.getString(1));
    }
    rs = this.metaData.getTables(null, null, null, tableTypes.toArray(new String[2]));
    int tableCount = 0;

    while(rs.next()){
      tableCount = tableCount + 1;
    }
    assertTrue(tableCount > 1);
  }

  @Test
  public void getNoTablesFilteredByEmptySetOfTypes() throws SQLException {
    final ResultSet rs = this.metaData.getTables(null, null, null, new String[0]);
    int tableCount = 0;

    while(rs.next()){
      tableCount = tableCount + 1;
    }
    assertEquals(0, tableCount);
  }

  @Test
  public void getSingleTables() throws SQLException {
    ResultSet rs = this.metaData.getTables(null, null, "ouser", null);
    int tableCount = 0;

    while (rs.next()) {
      tableCount = tableCount + 1;
    }
    assertEquals(1, tableCount);
  }

}
