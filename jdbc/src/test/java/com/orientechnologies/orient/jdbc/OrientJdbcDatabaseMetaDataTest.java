package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Before;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class OrientJdbcDatabaseMetaDataTest extends OrientJdbcBaseTest {

  private DatabaseMetaData metaData;

  @Before
  public void setup() throws SQLException {
    metaData = conn.getMetaData();

  }

  @Test
  public void verifyDriverAndDatabaseVersions() throws SQLException {

    assertEquals("memory:OrientJdbcDatabaseMetaDataTest", metaData.getURL());
    assertEquals("admin", metaData.getUserName());
    assertEquals("OrientDB", metaData.getDatabaseProductName());
    assertEquals(OConstants.ORIENT_VERSION, metaData.getDatabaseProductVersion());
    assertEquals(2, metaData.getDatabaseMajorVersion());
    assertEquals(2, metaData.getDatabaseMinorVersion());

    assertEquals("OrientDB JDBC Driver", metaData.getDriverName());
    assertEquals("OrientDB " + OConstants.getVersion() + " JDBC Driver", metaData.getDriverVersion());
    assertEquals(2, metaData.getDriverMajorVersion());
    assertEquals(2, metaData.getDriverMinorVersion());

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
    assertThat(Arrays.asList(keywordsStr.toUpperCase().split(",\\s*"))).contains("TRAVERSE");
  }

  @Test
  public void shouldRetrieveUniqueIndexInfoForTable() throws Exception {

    ResultSet indexInfo = metaData
        .getIndexInfo("OrientJdbcDatabaseMetaDataTest", "OrientJdbcDatabaseMetaDataTest", "Item", true, false);

    indexInfo.next();

    assertThat(indexInfo.getString("INDEX_NAME")).isEqualTo("Item.intKey");
    assertThat(indexInfo.getBoolean("NON_UNIQUE")).isFalse();

    indexInfo.next();

    assertThat(indexInfo.getString("INDEX_NAME")).isEqualTo("Item.stringKey");
    assertThat(indexInfo.getBoolean("NON_UNIQUE")).isFalse();

  }

  @Test
  public void getFields() throws SQLException {
    ResultSet rs = conn.createStatement().executeQuery("select from OUser");

    ResultSetMetaData rsMetaData = rs.getMetaData();

    int cc = rsMetaData.getColumnCount();
    Set<String> colset = new HashSet<String>();
    List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>(cc);
    for (int i = 1; i <= cc; i++) {
      String name = rsMetaData.getColumnLabel(i);
      //      if (colset.contains(name))
      //        continue;
      colset.add(name);
      Map<String, Object> field = new HashMap<String, Object>();
      field.put("name", name);

      try {
        String catalog = rsMetaData.getCatalogName(i);
        String schema = rsMetaData.getSchemaName(i);
        String table = rsMetaData.getTableName(i);
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

  @Test
  public void shouldFetchAllTables() throws SQLException {
    ResultSet rs = metaData.getTables(null, null, null, null);
    int tableCount = sizeOf(rs);

    assertThat(tableCount).isEqualTo(16);

  }

  @Test
  public void shouldFillSchemaAndCatalogWithDatabaseName() throws SQLException {
    ResultSet rs = metaData.getTables(null, null, null, null);

    while (rs.next()) {
      assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("OrientJdbcDatabaseMetaDataTest");
      assertThat(rs.getString("TABLE_CAT")).isEqualTo("OrientJdbcDatabaseMetaDataTest");
    }

  }

  @Test
  public void shouldGetAllTablesFilteredByAllTypes() throws SQLException {
    ResultSet rs = metaData.getTableTypes();
    List<String> tableTypes = new ArrayList<String>(2);
    while (rs.next()) {
      tableTypes.add(rs.getString(1));
    }
    rs = metaData.getTables(null, null, null, tableTypes.toArray(new String[2]));
    int tableCount = sizeOf(rs);
    assertThat(tableCount).isEqualTo(16);
  }

  @Test
  public void getNoTablesFilteredByEmptySetOfTypes() throws SQLException {
    final ResultSet rs = metaData.getTables(null, null, null, new String[0]);
    int tableCount = sizeOf(rs);

    assertThat(tableCount).isEqualTo(0);
  }

  @Test
  public void getSingleTable() throws SQLException {
    ResultSet rs = metaData.getTables(null, null, "ouser", null);
    rs.next();
    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {

      System.out.println(
          rs.getMetaData().getColumnName(i));
    }
    assertThat(rs.getString("TABLE_NAME")).isEqualTo("OUser");
    assertThat(rs.getString("TABLE_CAT")).isEqualTo("OrientJdbcDatabaseMetaDataTest");
    assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("OrientJdbcDatabaseMetaDataTest");
    assertThat(rs.getString("REMARKS")).isNull();
    assertThat(rs.getString("REF_GENERATION")).isNull();
    assertThat(rs.getString("TYPE_NAME")).isNull();

//
    assertThat(rs.next()).isFalse();
  }

  @Test
  public void shouldGetSingleColumnOfArticle() throws SQLException {
    ResultSet rs = metaData.getColumns(null, null, "Article", "uuid");
    rs.next();

    assertThat(rs.getString("TABLE_NAME")).isEqualTo("Article");
    assertThat(rs.getString("COLUMN_NAME")).isEqualTo("uuid");
    assertThat(rs.getString("TYPE_NAME")).isEqualTo("INTEGER");
    assertThat(rs.getInt("DATA_TYPE")).isEqualTo(4);

    assertThat(rs.next()).isFalse();
  }

  @Test
  public void shouldGetAllColumnsOfArticle() throws SQLException {
    ResultSet rs = metaData.getColumns(null, null, "Article", null);

    while (rs.next()) {
      assertThat(rs.getString("TABLE_NAME")).isEqualTo("Article");
      assertThat(rs.getString("COLUMN_NAME")).isIn("date", "uuid", "author", "title", "content");
      assertThat(rs.getInt("DATA_TYPE")).isIn(9, 12, 4, 91, 2000);
      assertThat(rs.getString("TYPE_NAME")).isIn("LINK", "DATE", "STRING", "INTEGER");

    }
  }

  @Test
  public void shouldGetAllIndexesOnArticle() throws Exception {
    ResultSet rs = metaData.getIndexInfo(null, null, "Article", true, true);

    rs.next();

    assertThat(rs.getString("COLUMN_NAME")).isEqualTo("uuid");
    assertThat(rs.getString("INDEX_NAME")).isEqualTo("Article.uuid");
    assertThat(rs.getBoolean("NON_UNIQUE")).isFalse();

  }

  @Test
  public void shouldGetPrimaryKeyOfArticle() throws Exception {
    ResultSet rs = metaData.getPrimaryKeys(null, null, "Article");

    rs.next();
    assertThat(rs.getString("TABLE_NAME")).isEqualTo("Article");
    assertThat(rs.getString("COLUMN_NAME")).isEqualTo("uuid");
    assertThat(rs.getString("PK_NAME")).isEqualTo("Article.uuid");
    assertThat(rs.getInt("KEY_SEQ")).isEqualTo(1);

  }

  private int sizeOf(ResultSet rs) throws SQLException {
    int tableCount = 0;

    while (rs.next()) {
      tableCount++;
    }
    return tableCount;
  }

}
