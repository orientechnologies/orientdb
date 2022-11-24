/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://orientdb.com
 */
package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * @author Roberto Franchini (CELI srl - franchini--at--celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (OrientDB - l.garulli--at--orientdb.com)
 */
public class OrientJdbcDatabaseMetaData implements DatabaseMetaData {
  protected static final List<String> TABLE_TYPES = Arrays.asList("TABLE", "SYSTEM TABLE");
  private final OrientJdbcConnection connection;
  private final ODatabaseDocumentInternal database;

  public OrientJdbcDatabaseMetaData(
      OrientJdbcConnection iConnection, ODatabaseDocumentInternal iDatabase) {
    connection = iConnection;
    database = iDatabase;
  }

  public boolean allProceduresAreCallable() throws SQLException {
    return true;
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  public String getURL() throws SQLException {
    database.activateOnCurrentThread();
    return database.getURL();
  }

  public String getUserName() throws SQLException {
    database.activateOnCurrentThread();
    return database.getUser().getName();
  }

  public boolean isReadOnly() throws SQLException {

    return false;
  }

  public boolean nullsAreSortedHigh() throws SQLException {

    return false;
  }

  public boolean nullsAreSortedLow() throws SQLException {

    return false;
  }

  public boolean nullsAreSortedAtStart() throws SQLException {

    return false;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {

    return false;
  }

  public String getDatabaseProductName() throws SQLException {
    return "OrientDB";
  }

  public String getDatabaseProductVersion() throws SQLException {
    return OConstants.getVersion();
  }

  public String getDriverName() throws SQLException {
    return "OrientDB JDBC Driver";
  }

  public String getDriverVersion() throws SQLException {
    return OrientJdbcDriver.getVersion();
  }

  public int getDriverMajorVersion() {
    return OConstants.getVersionMajor();
  }

  public int getDriverMinorVersion() {
    return OConstants.getVersionMinor();
  }

  public boolean usesLocalFiles() throws SQLException {

    return false;
  }

  public boolean usesLocalFilePerTable() throws SQLException {

    return false;
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {

    return false;
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {

    return false;
  }

  public String getIdentifierQuoteString() throws SQLException {
    return " ";
  }

  public String getSQLKeywords() throws SQLException {
    return "@rid,@class,@version,@size,@type,@this,CONTAINS,CONTAINSALL,CONTAINSKEY,"
        + "CONTAINSVALUE,CONTAINSTEXT,MATCHES,TRAVERSE";
  }

  public String getNumericFunctions() throws SQLException {

    return null;
  }

  public String getStringFunctions() throws SQLException {

    return "";
  }

  public String getSystemFunctions() throws SQLException {

    return "";
  }

  public String getTimeDateFunctions() throws SQLException {
    return "date,sysdate";
  }

  public String getSearchStringEscape() throws SQLException {

    return null;
  }

  public String getExtraNameCharacters() throws SQLException {
    return null;
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {

    return false;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {

    return false;
  }

  public boolean supportsColumnAliasing() throws SQLException {

    return false;
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {

    return false;
  }

  public boolean supportsConvert() throws SQLException {

    return false;
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {

    return false;
  }

  public boolean supportsTableCorrelationNames() throws SQLException {

    return false;
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {

    return false;
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {

    return false;
  }

  public boolean supportsOrderByUnrelated() throws SQLException {

    return false;
  }

  public boolean supportsGroupBy() throws SQLException {

    return true;
  }

  public boolean supportsGroupByUnrelated() throws SQLException {

    return false;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {

    return false;
  }

  public boolean supportsLikeEscapeClause() throws SQLException {

    return false;
  }

  public boolean supportsMultipleResultSets() throws SQLException {

    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {

    return true;
  }

  public boolean supportsNonNullableColumns() throws SQLException {

    return true;
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {

    return false;
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {

    return false;
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {

    return false;
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {

    return false;
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {

    return false;
  }

  public boolean supportsANSI92FullSQL() throws SQLException {

    return false;
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {

    return false;
  }

  public boolean supportsOuterJoins() throws SQLException {

    return false;
  }

  public boolean supportsFullOuterJoins() throws SQLException {

    return false;
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {

    return false;
  }

  public String getSchemaTerm() throws SQLException {

    return null;
  }

  public String getProcedureTerm() throws SQLException {
    return "Function";
  }

  public String getCatalogTerm() throws SQLException {

    return null;
  }

  public boolean isCatalogAtStart() throws SQLException {

    return false;
  }

  public String getCatalogSeparator() throws SQLException {

    return null;
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {

    return false;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {

    return false;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsPositionedDelete() throws SQLException {

    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {

    return false;
  }

  public boolean supportsSelectForUpdate() throws SQLException {

    return false;
  }

  public boolean supportsStoredProcedures() throws SQLException {

    return true;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {

    return false;
  }

  public boolean supportsSubqueriesInExists() throws SQLException {

    return false;
  }

  public boolean supportsSubqueriesInIns() throws SQLException {

    return true;
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {

    return false;
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {

    return false;
  }

  public boolean supportsUnion() throws SQLException {

    return true;
  }

  public boolean supportsUnionAll() throws SQLException {

    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {

    return false;
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {

    return false;
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {

    return false;
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {

    return false;
  }

  public int getMaxBinaryLiteralLength() throws SQLException {

    return 0;
  }

  public int getMaxCharLiteralLength() throws SQLException {

    return 0;
  }

  public int getMaxColumnNameLength() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInIndex() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInOrderBy() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInSelect() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInTable() throws SQLException {

    return 0;
  }

  public int getMaxConnections() throws SQLException {

    return 0;
  }

  public int getMaxCursorNameLength() throws SQLException {

    return 0;
  }

  public int getMaxIndexLength() throws SQLException {

    return 0;
  }

  public int getMaxSchemaNameLength() throws SQLException {
    return 0;
  }

  public int getMaxProcedureNameLength() throws SQLException {

    return 0;
  }

  public int getMaxCatalogNameLength() throws SQLException {

    return 0;
  }

  public int getMaxRowSize() throws SQLException {
    return 0;
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {

    return false;
  }

  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  public int getMaxStatements() throws SQLException {
    return 0;
  }

  public int getMaxTableNameLength() throws SQLException {
    return 1024;
  }

  public int getMaxTablesInSelect() throws SQLException {
    return 1;
  }

  public int getMaxUserNameLength() throws SQLException {

    return 0;
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return java.sql.Connection.TRANSACTION_NONE;
  }

  public boolean supportsTransactions() throws SQLException {

    return true;
  }

  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {

    return false;
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {

    return false;
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {

    return false;
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return true;
  }

  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();
    final OInternalResultSet resultSet = new OInternalResultSet();

    OFunctionLibrary functionLibrary = database.getMetadata().getFunctionLibrary();

    for (String functionName : functionLibrary.getFunctionNames()) {

      if (OrientJdbcUtils.like(functionName, procedureNamePattern)) {
        OResultInternal element = new OResultInternal();
        element.setProperty("PROCEDURE_CAT", (Object) null);
        element.setProperty("PROCEDURE_SCHEM", (Object) null);
        element.setProperty("PROCEDURE_NAME", functionName);
        element.setProperty("REMARKS", "");
        element.setProperty("PROCEDURE_TYPE", procedureResultUnknown);
        element.setProperty("SPECIFIC_NAME", functionName);

        resultSet.add(element);
      }
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();

    final OInternalResultSet resultSet = new OInternalResultSet();
    OFunctionLibrary functionLibrary = database.getMetadata().getFunctionLibrary();

    for (String functionName : functionLibrary.getFunctionNames()) {

      if (OrientJdbcUtils.like(functionName, procedureNamePattern)) {

        final OFunction f = functionLibrary.getFunction(procedureNamePattern);

        for (String p : f.getParameters()) {
          final OResultInternal doc = new OResultInternal();
          doc.setProperty("PROCEDURE_CAT", database.getName());
          doc.setProperty("PROCEDURE_SCHEM", database.getName());
          doc.setProperty("PROCEDURE_NAME", f.getName());
          doc.setProperty("COLUMN_NAME", p);
          doc.setProperty("COLUMN_TYPE", procedureColumnIn);
          doc.setProperty("DATA_TYPE", java.sql.Types.OTHER);
          doc.setProperty("SPECIFIC_NAME", f.getName());

          resultSet.add(doc);
        }

        final OResultInternal doc = new OResultInternal();

        doc.setProperty("PROCEDURE_CAT", database.getName());
        doc.setProperty("PROCEDURE_SCHEM", database.getName());
        doc.setProperty("PROCEDURE_NAME", f.getName());
        doc.setProperty("COLUMN_NAME", "return");
        doc.setProperty("COLUMN_TYPE", procedureColumnReturn);
        doc.setProperty("DATA_TYPE", java.sql.Types.OTHER);
        doc.setProperty("SPECIFIC_NAME", f.getName());

        resultSet.add(doc);
      }
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    database.activateOnCurrentThread();
    final Collection<OClass> classes = database.getMetadata().getSchema().getClasses();

    OInternalResultSet resultSet = new OInternalResultSet();

    final List tableTypes = types != null ? Arrays.asList(types) : TABLE_TYPES;
    for (OClass cls : classes) {
      final String className = cls.getName();
      final String type;

      if (OMetadataInternal.SYSTEM_CLUSTER.contains(cls.getName().toLowerCase(Locale.ENGLISH)))
        type = "SYSTEM TABLE";
      else type = "TABLE";

      if (tableTypes.contains(type)
          && (tableNamePattern == null
              || tableNamePattern.equals("%")
              || tableNamePattern.equalsIgnoreCase(className))) {

        OResultInternal doc = new OResultInternal();

        doc.setProperty("TABLE_CAT", database.getName());
        doc.setProperty("TABLE_SCHEM", database.getName());
        doc.setProperty("TABLE_NAME", className);
        doc.setProperty("TABLE_TYPE", type);
        doc.setProperty("REMARKS", (Object) null);
        doc.setProperty("TYPE_NAME", (Object) null);
        doc.setProperty("REF_GENERATION", (Object) null);
        resultSet.add(doc);
      }
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    database.activateOnCurrentThread();
    OInternalResultSet resultSet = new OInternalResultSet();

    final OResultInternal field = new OResultInternal();
    field.setProperty("TABLE_SCHEM", database.getName());
    field.setProperty("TABLE_CATALOG", database.getName());

    resultSet.add(field);

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getCatalogs() throws SQLException {
    database.activateOnCurrentThread();

    OInternalResultSet resultSet = new OInternalResultSet();

    final OResultInternal field = new OResultInternal();
    field.setProperty("TABLE_CAT", database.getName());

    resultSet.add(field);

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    database.activateOnCurrentThread();

    OInternalResultSet resultSet = new OInternalResultSet();
    for (String tableType : TABLE_TYPES) {
      final OResultInternal field = new OResultInternal();
      field.setProperty("TABLE_TYPE", tableType);
      resultSet.add(field);
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getColumns(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String columnNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();

    final OInternalResultSet resultSet = new OInternalResultSet();

    final OSchema schema = database.getMetadata().getImmutableSchemaSnapshot();

    for (OClass clazz : schema.getClasses()) {
      if (OrientJdbcUtils.like(clazz.getName(), tableNamePattern)) {
        for (OProperty prop : clazz.properties()) {
          if (columnNamePattern == null) {
            resultSet.add(getPropertyAsDocument(clazz, prop));
          } else {
            if (OrientJdbcUtils.like(prop.getName(), columnNamePattern)) {
              resultSet.add(getPropertyAsDocument(clazz, prop));
            }
          }
        }
      }
    }
    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getColumnPrivileges(
      final String catalog, final String schema, final String table, final String columnNamePattern)
      throws SQLException {
    return getEmptyResultSet();
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {

    return getEmptyResultSet();
  }

  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {

    return getEmptyResultSet();
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {

    return getEmptyResultSet();
  }

  @Override
  public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    database.activateOnCurrentThread();
    final Set<OIndex> classIndexes =
        database.getMetadata().getIndexManagerInternal().getClassIndexes(database, table);

    final Set<OIndex> uniqueIndexes = new HashSet<>();

    for (OIndex oIndex : classIndexes) {
      if (oIndex.getType().equals(INDEX_TYPE.UNIQUE.name())) uniqueIndexes.add(oIndex);
    }

    final OInternalResultSet resultSet = new OInternalResultSet();

    for (OIndex unique : uniqueIndexes) {
      int keyFiledSeq = 1;
      for (String keyFieldName : unique.getDefinition().getFields()) {
        final OResultInternal res = new OResultInternal();
        res.setProperty("TABLE_CAT", catalog);
        res.setProperty("TABLE_SCHEM", catalog);
        res.setProperty("TABLE_NAME", table);
        res.setProperty("COLUMN_NAME", keyFieldName);
        res.setProperty("KEY_SEQ", keyFiledSeq);
        res.setProperty("PK_NAME", unique.getName());
        keyFiledSeq++;

        resultSet.add(res);
      }
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {

    database.activateOnCurrentThread();

    OClass aClass = database.getMetadata().getSchema().getClass(table);

    aClass.declaredProperties().stream().forEach(p -> p.getType());
    return getEmptyResultSet();
  }

  private ResultSet getEmptyResultSet() throws SQLException {
    database.activateOnCurrentThread();

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        new OInternalResultSet(),
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {

    return getEmptyResultSet();
  }

  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {

    return getEmptyResultSet();
  }

  public ResultSet getTypeInfo() throws SQLException {
    final OInternalResultSet resultSet = new OInternalResultSet();

    OResultInternal res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.BINARY.toString());
    res.setProperty("DATA_TYPE", Types.BINARY);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.BOOLEAN.toString());
    res.setProperty("DATA_TYPE", Types.BOOLEAN);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.BYTE.toString());
    res.setProperty("DATA_TYPE", Types.TINYINT);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("UNSIGNED_ATTRIBUTE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.DATE.toString());
    res.setProperty("DATA_TYPE", Types.DATE);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.DATETIME.toString());
    res.setProperty("DATA_TYPE", Types.DATE);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.DECIMAL.toString());
    res.setProperty("DATA_TYPE", Types.DECIMAL);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("UNSIGNED_ATTRIBUTE", false);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.FLOAT.toString());
    res.setProperty("DATA_TYPE", Types.FLOAT);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("UNSIGNED_ATTRIBUTE", false);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.DOUBLE.toString());
    res.setProperty("DATA_TYPE", Types.DOUBLE);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("UNSIGNED_ATTRIBUTE", false);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.EMBEDDED.toString());
    res.setProperty("DATA_TYPE", Types.STRUCT);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.EMBEDDEDLIST.toString());
    res.setProperty("DATA_TYPE", Types.ARRAY);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.INTEGER.toString());
    res.setProperty("DATA_TYPE", Types.INTEGER);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("UNSIGNED_ATTRIBUTE", false);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.LINKLIST.toString());
    res.setProperty("DATA_TYPE", Types.ARRAY);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.LONG.toString());
    res.setProperty("DATA_TYPE", Types.BIGINT);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("UNSIGNED_ATTRIBUTE", false);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.STRING.toString());
    res.setProperty("DATA_TYPE", Types.VARCHAR);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    res = new OResultInternal();
    res.setProperty("TYPE_NAME", OType.SHORT.toString());
    res.setProperty("DATA_TYPE", Types.SMALLINT);
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable);
    res.setProperty("CASE_SENSITIVE", true);
    res.setProperty("UNSIGNED_ATTRIBUTE", false);
    res.setProperty("SEARCHABLE", true);
    resultSet.add(res);

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    database.activateOnCurrentThread();
    OMetadataInternal metadata = database.getMetadata();
    if (!approximate) {
      metadata.getIndexManagerInternal().reload();
    }

    final Set<OIndex> classIndexes =
        metadata.getIndexManagerInternal().getClassIndexes(database, table);

    final Set<OIndex> indexes = new HashSet<>();

    for (OIndex oIndex : classIndexes) {
      if (!unique || oIndex.getType().equals(INDEX_TYPE.UNIQUE.name())) indexes.add(oIndex);
    }

    final OInternalResultSet resultSet = new OInternalResultSet();
    for (OIndex idx : indexes) {
      boolean notUniqueIndex = !(idx.getType().equals(INDEX_TYPE.UNIQUE.name()));

      final String fieldNames = idx.getDefinition().getFields().toString();

      OResultInternal res = new OResultInternal();
      res.setProperty("TABLE_CAT", catalog);
      res.setProperty("TABLE_SCHEM", schema);
      res.setProperty("TABLE_NAME", table);
      res.setProperty("NON_UNIQUE", notUniqueIndex);
      res.setProperty("INDEX_QUALIFIER", (Object) null);
      res.setProperty("INDEX_NAME", idx.getName());
      res.setProperty("TYPE", idx.getType());
      res.setProperty("ORDINAL_POSITION", 0);
      res.setProperty("COLUMN_NAME", fieldNames.substring(1, fieldNames.length() - 1));
      res.setProperty("ASC_OR_DESC", "ASC");

      resultSet.add(res);
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public boolean supportsResultSetType(int type) throws SQLException {

    return false;
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {

    return false;
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean updatesAreDetected(int type) throws SQLException {

    return false;
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean insertsAreDetected(int type) throws SQLException {

    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {

    return false;
  }

  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    database.activateOnCurrentThread();
    final Collection<OClass> classes = database.getMetadata().getSchema().getClasses();

    OInternalResultSet resultSet = new OInternalResultSet();
    for (OClass cls : classes) {
      final OResultInternal res = new OResultInternal();
      res.setProperty("TYPE_CAT", (Object) null);
      res.setProperty("TYPE_SCHEM", (Object) null);
      res.setProperty("TYPE_NAME", cls.getName());
      res.setProperty("CLASS_NAME", cls.getName());
      res.setProperty("DATA_TYPE", java.sql.Types.STRUCT);
      res.setProperty("REMARKS", (Object) null);
      resultSet.add(res);
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public boolean supportsSavepoints() throws SQLException {

    return false;
  }

  public boolean supportsNamedParameters() throws SQLException {

    return true;
  }

  public boolean supportsMultipleOpenResults() throws SQLException {

    return false;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {

    return false;
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();
    final OClass cls = database.getMetadata().getSchema().getClass(typeNamePattern);

    final OInternalResultSet resultSet = new OInternalResultSet();
    if (cls != null && cls.getSuperClass() != null) {
      final OResultInternal res = new OResultInternal();
      res.setProperty("TABLE_CAT", catalog);
      res.setProperty("TABLE_SCHEM", catalog);
      res.setProperty("TABLE_NAME", cls.getName());
      res.setProperty("SUPERTYPE_CAT", catalog);
      res.setProperty("SUPERTYPE_SCHEM", catalog);
      res.setProperty("SUPERTYPE_NAME", cls.getSuperClass().getName());
      resultSet.add(res);
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getSuperTables(
      final String catalog, final String schemaPattern, final String tableNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();
    final OClass cls = database.getMetadata().getSchema().getClass(tableNamePattern);
    final OInternalResultSet resultSet = new OInternalResultSet();

    if (cls != null && cls.getSuperClass() != null) {
      final OResultInternal res = new OResultInternal();

      res.setProperty("TABLE_CAT", catalog);
      res.setProperty("TABLE_SCHEM", catalog);
      res.setProperty("TABLE_NAME", cls.getName());
      res.setProperty("SUPERTABLE_CAT", catalog);
      res.setProperty("SUPERTABLE_SCHEM", catalog);
      res.setProperty("SUPERTABLE_NAME", cls.getSuperClass().getName());
      resultSet.add(res);
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {

    return getEmptyResultSet();
  }

  public boolean supportsResultSetHoldability(int holdability) throws SQLException {

    return false;
  }

  public int getResultSetHoldability() throws SQLException {

    return 0;
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return OConstants.getVersionMajor();
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return OConstants.getVersionMinor();
  }

  public int getJDBCMajorVersion() throws SQLException {

    return 0;
  }

  public int getJDBCMinorVersion() throws SQLException {

    return 0;
  }

  public int getSQLStateType() throws SQLException {

    return 0;
  }

  public boolean locatorsUpdateCopy() throws SQLException {

    return false;
  }

  public boolean supportsStatementPooling() throws SQLException {

    return false;
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {

    return null;
  }

  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {

    return getEmptyResultSet();
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {

    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  public ResultSet getClientInfoProperties() throws SQLException {

    return getEmptyResultSet();
  }

  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {

    database.activateOnCurrentThread();
    OInternalResultSet resultSet = new OInternalResultSet();
    for (String fName : database.getMetadata().getFunctionLibrary().getFunctionNames()) {
      final OResultInternal res = new OResultInternal();
      res.setProperty("FUNCTION_CAT", (Object) null);
      res.setProperty("FUNCTION_SCHEM", (Object) null);
      res.setProperty("FUNCTION_NAME", fName);
      res.setProperty("REMARKS", "");
      res.setProperty("FUNCTION_TYPE", procedureResultUnknown);
      res.setProperty("SPECIFIC_NAME", fName);

      resultSet.add(res);
    }

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();
    final OInternalResultSet resultSet = new OInternalResultSet();

    final OFunction f =
        database.getMetadata().getFunctionLibrary().getFunction(functionNamePattern);

    for (String p : f.getParameters()) {
      final OResultInternal res = new OResultInternal();
      res.setProperty("FUNCTION_CAT", (Object) null);
      res.setProperty("FUNCTION_SCHEM", (Object) null);
      res.setProperty("FUNCTION_NAME", f.getName());
      res.setProperty("COLUMN_NAME", p);
      res.setProperty("COLUMN_TYPE", procedureColumnIn);
      res.setProperty("DATA_TYPE", java.sql.Types.OTHER);
      res.setProperty("SPECIFIC_NAME", f.getName());
      resultSet.add(res);
    }

    final OResultInternal res = new OResultInternal();
    res.setProperty("FUNCTION_CAT", (Object) null);
    res.setProperty("FUNCTION_SCHEM", (Object) null);
    res.setProperty("FUNCTION_NAME", f.getName());
    res.setProperty("COLUMN_NAME", "return");
    res.setProperty("COLUMN_TYPE", procedureColumnReturn);
    res.setProperty("DATA_TYPE", java.sql.Types.OTHER);
    res.setProperty("SPECIFIC_NAME", f.getName());

    resultSet.add(res);

    return new OrientJdbcResultSet(
        new OrientJdbcStatement(connection),
        resultSet,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getPseudoColumns(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    return getEmptyResultSet();
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  private OResultInternal getPropertyAsDocument(final OClass clazz, final OProperty prop) {
    database.activateOnCurrentThread();
    final OType type = prop.getType();
    OResultInternal res = new OResultInternal();
    res.setProperty("TABLE_CAT", database.getName());
    res.setProperty("TABLE_SCHEM", database.getName());
    res.setProperty("TABLE_NAME", clazz.getName());
    res.setProperty("COLUMN_NAME", prop.getName());
    res.setProperty("DATA_TYPE", OrientJdbcResultSetMetaData.getSqlType(type));
    res.setProperty("TYPE_NAME", type.name());
    res.setProperty("COLUMN_SIZE", 1);
    res.setProperty("BUFFER_LENGTH", null);
    res.setProperty("DECIMAL_DIGITS", null);
    res.setProperty("NUM_PREC_RADIX", 10);
    res.setProperty("NULLABLE", !prop.isNotNull() ? columnNoNulls : columnNullable);
    res.setProperty("REMARKS", prop.getDescription());
    res.setProperty("COLUMN_DEF", prop.getDefaultValue());
    res.setProperty("SQL_DATA_TYPE", null);
    res.setProperty("SQL_DATETIME_SUB", null);
    res.setProperty("CHAR_OCTET_LENGTH", null);
    res.setProperty("ORDINAL_POSITION", prop.getId());
    res.setProperty("IS_NULLABLE", prop.isNotNull() ? "NO" : "YES");

    return res;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {

    return null;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {

    return false;
  }
}
