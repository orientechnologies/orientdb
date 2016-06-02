/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.loader;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLPipeline;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.orientechnologies.orient.etl.OETLProcessor.LOG_LEVELS.*;
import static com.orientechnologies.orient.etl.loader.OOrientDBLoader.DB_TYPE.DOCUMENT;
import static com.orientechnologies.orient.etl.loader.OOrientDBLoader.DB_TYPE.GRAPH;

/**
 * ETL Loader that saves record into OrientDB database.
 */
public class OOrientDBLoader extends OAbstractLoader implements OLoader {

  private static String NOT_DEF = "not_defined";
  protected String          clusterName;
  protected String          className;
  protected List<ODocument> classes;
  protected List<ODocument> indexes;
  protected OClass          schemaClass;
  protected String          dbURL;
  protected String dbUser     = "admin";
  protected String dbPassword = "admin";

  protected String serverUser     = NOT_DEF;
  protected String serverPassword = NOT_DEF;

  protected boolean    dbAutoCreate               = true;
  protected boolean    dbAutoDropIfExists         = false;
  protected boolean    dbAutoCreateProperties     = false;
  protected boolean    useLightweightEdges        = false;
  protected boolean    standardElementConstraints = true;
  protected boolean    tx                         = false;
  protected int        batchCommitSize            = 0;
  protected AtomicLong batchCounter               = new AtomicLong(0);
  protected DB_TYPE    dbType                     = DOCUMENT;
  protected boolean    wal                        = true;
  protected boolean    txUseLog                   = false;

  public OOrientDBLoader() {
  }

  @Override
  public void load(OETLPipeline pipeline, final Object input, OCommandContext context) {
    if (input == null)
      return;

    if (dbAutoCreateProperties) {
      autoCreateProperties(pipeline, input);
    }

    if (tx && dbType == DOCUMENT) {
      final ODatabaseDocument documentDatabase = pipeline.getDocumentDatabase();
      if (!documentDatabase.getTransaction().isActive()) {
        documentDatabase.begin();
        documentDatabase.getTransaction().setUsingLog(txUseLog);

      }
    }

    if (input instanceof OrientVertex) {
      final OrientVertex v = (OrientVertex) input;

      v.save(clusterName);

    } else if (input instanceof ODocument) {
      final ODocument doc = (ODocument) input;

      doc.setClassName(className);

      doc.save(clusterName);
    }

    progress.incrementAndGet();

    // DO BATCH COMMIT
    if (batchCommitSize > 0 && batchCounter.get() > batchCommitSize) {
      if (dbType == DOCUMENT) {
        final ODatabaseDocument documentDatabase = pipeline.getDocumentDatabase();
        log(DEBUG, "committing batch");
        documentDatabase.commit();
        documentDatabase.begin();
        documentDatabase.getTransaction().setUsingLog(txUseLog);
      } else {
        log(DEBUG, "committing batch");
        pipeline.getGraphDatabase().commit();
      }
      batchCounter.set(0);
    } else {
      batchCounter.incrementAndGet();
    }
  }

  private void autoCreateProperties(OETLPipeline pipeline, Object input) {
    if (dbType == DOCUMENT && input instanceof ODocument) {
      auroCreatePropertiesOnDocument(pipeline, (ODocument) input);
    } else if (dbType == GRAPH && input instanceof OrientElement) {
      autoCreatePropertiesOnElement(pipeline, (OrientElement) input);
    }
  }

  private void autoCreatePropertiesOnElement(OETLPipeline pipeline, OrientElement element) {

    final OClass cls;
    final String clsName =
        className != null ? className : (element instanceof OrientVertex ? element.getLabel() : element.getLabel());

    if (clsName != null)
      cls = getOrCreateClass(pipeline, clsName, element.getBaseClassName());
    else
      throw new IllegalArgumentException("No class defined on graph element: " + element);

    for (String f : element.getPropertyKeys()) {
      final String newName = transformFieldName(f);
      final String fName = newName != null ? newName : f;

      OProperty p = cls.getProperty(fName);
      if (p == null) {
        final Object fValue = element.getProperty(f);
        createProperty(cls, fName, fValue);
        if (newName != null) {
          // REPLACE IT
          element.removeProperty(f);
          element.setProperty(newName, fValue);
        }
      }
    }
  }

  private void auroCreatePropertiesOnDocument(OETLPipeline pipeline, ODocument doc) {
    final OClass cls;
    if (className != null)
      cls = getOrCreateClass(pipeline, className, null);
    else
      cls = doc.getSchemaClass();

    for (String f : doc.fieldNames()) {
      final String newName = transformFieldName(f);
      final String fName = newName != null ? newName : f;

      OProperty p = cls.getProperty(fName);
      if (p == null) {
        final Object fValue = doc.field(f);
        createProperty(cls, fName, fValue);
        if (newName != null) {
          // REPLACE IT
          doc.removeField(f);
          doc.field(newName, fValue);
        }
      }
    }
  }

  @Override
  public String getUnit() {
    return dbType == DOCUMENT ? "documents" : "vertices";
  }

  @Override
  public void rollback(OETLPipeline pipeline) {
    if (tx)
      if (dbType == DOCUMENT) {
        final ODatabaseDocument documentDatabase = pipeline.getDocumentDatabase();
        if (documentDatabase.getTransaction().isActive())
          documentDatabase.rollback();
      } else
        pipeline.getGraphDatabase().rollback();
  }

  protected OClass getOrCreateClass(OETLPipeline pipeline, final String iClassName, final String iSuperClass) {
    OClass cls;

    if (dbType == DOCUMENT) {
      cls = getOrCreateClassOnDocument(pipeline, iClassName, iSuperClass);

    } else {
      cls = getOrCreateClassOnGraph(pipeline, iClassName, iSuperClass);

    }

    if (clusterName != null) {
      int clusterIdByName = pipeline.getDocumentDatabase().getClusterIdByName(clusterName);
      if (clusterIdByName == -1) {
        cls.addCluster(clusterName);
      }
    }
    return cls;
  }

  private OClass getOrCreateClassOnGraph(OETLPipeline pipeline, String iClassName, String iSuperClass) {
    OClass cls;// GRAPH
    final OrientBaseGraph graphDatabase = pipeline.getGraphDatabase();
    OSchemaProxy schema = graphDatabase.getRawGraph().getMetadata().getSchema();
    cls = schema.getClass(iClassName);

    if (cls == null) {

      if (iSuperClass != null) {
        final OClass superClass = graphDatabase.getRawGraph().getMetadata().getSchema().getClass(iSuperClass);
        if (superClass == null)
          throw new OLoaderException("Cannot find super class '" + iSuperClass + "'");

        if (graphDatabase.getVertexBaseType().isSuperClassOf(superClass)) {
          // VERTEX
          cls = graphDatabase.createVertexType(iClassName, superClass);
          log(DEBUG, "- OrientDBLoader: created vertex class '%s' extends '%s'", iClassName, iSuperClass);
        } else {
          // EDGE
          cls = graphDatabase.createEdgeType(iClassName, superClass);
          log(DEBUG, "- OrientDBLoader: created edge class '%s' extends '%s'", iClassName, iSuperClass);
        }
      } else {
        // ALWAYS CREATE SUB-VERTEX
        cls = graphDatabase.createVertexType(iClassName);
        log(DEBUG, "- OrientDBLoader: created vertex class '%s'", iClassName);
      }
    }
    return cls;
  }

  private OClass getOrCreateClassOnDocument(OETLPipeline pipeline, String iClassName, String iSuperClass) {
    OClass cls;// DOCUMENT
    final ODatabaseDocument documentDatabase = pipeline.getDocumentDatabase();
    if (documentDatabase.getMetadata().getSchema().existsClass(iClassName))
      cls = documentDatabase.getMetadata().getSchema().getClass(iClassName);
    else {
      if (iSuperClass != null) {
        final OClass superClass = documentDatabase.getMetadata().getSchema().getClass(iSuperClass);
        if (superClass == null)
          throw new OLoaderException("Cannot find super class '" + iSuperClass + "'");

        cls = documentDatabase.getMetadata().getSchema().createClass(iClassName, superClass);
        log(DEBUG, "- OrientDBLoader: created class '%s' extends '%s'", iClassName, iSuperClass);
      } else {
        cls = documentDatabase.getMetadata().getSchema().createClass(iClassName);
        log(DEBUG, "- OrientDBLoader: created class '%s'", iClassName);
      }
    }
    return cls;
  }

  private String transformFieldName(String f) {
    final char first = f.charAt(0);
    if (Character.isDigit(first))
      return "field" + Character.toUpperCase(first) + (f.length() > 1 ? f.substring(1) : "");
    return null;
  }

  protected void createProperty(final OClass cls, final String f, final Object fValue) {
    if (fValue != null) {
      final OType fType = OType.getTypeByClass(fValue.getClass());

      try {
        cls.createProperty(f, fType);
      } catch (OSchemaException e) {
      }

      log(DEBUG, "created property [%s.%s] of type [%s]", cls.getName(), f, fType);
    }
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + "{dbUrl:{optional:false,description:'Database URL'}},"
        + "{dbUser:{optional:true,description:'Database user, default is admin'}},"
        + "{dbPassword:{optional:true,description:'Database password, default is admin'}},"
        + "{dbType:{optional:true,description:'Database type, default is document',values:" + stringArray2Json(DB_TYPE.values())
        + "}}," + "{class:{optional:true,description:'Record class name'}},"
        + "{tx:{optional:true,description:'Transaction mode: true executes in transaction, false for atomic operations'}},"
        + "{dbAutoCreate:{optional:true,description:'Auto create the database if not exists. Default is true'}},"
        + "{dbAutoCreateProperties:{optional:true,description:'Auto create properties in schema'}},"
        + "{dbAutoDropIfExists:{optional:true,description:'Auto drop the database if already exists. Default is false.'}},"
        + "{batchCommit:{optional:true,description:'Auto commit every X items. This speed up creation of edges.'}},"
        + "{wal:{optional:true,description:'Use the WAL (Write Ahead Log)'}},"
        + "{useLightweightEdges:{optional:true,description:'Enable/Disable LightweightEdges in Graphs. Default is false'}},"
        + "{standardElementConstraints:{optional:true,description:'Enable/Disable Standard Blueprints constraints on names. Default is true'}},"
        + "{cluster:{optional:true,description:'Cluster name where to store the new record'}},"
        + "{settings:{optional:true,description:'OrientDB settings as a map'}},"
        + "{classes:{optional:true,description:'Classes used. It assure the classes exist or in case create them'}},"
        + "{indexes:{optional:true,description:'Indexes used. It assure the indexes exist or in case create them'}}],"
        + "input:['OrientVertex','ODocument']}");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument conf, final OCommandContext iContext) {
    super.configure(iProcessor, conf, iContext);

    if (conf.containsField("dbURL"))
      dbURL = (String) resolve(conf.field("dbURL"));

    if (conf.containsField("dbUser"))
      dbUser = (String) resolve(conf.field("dbUser"));
    if (conf.containsField("dbPassword"))
      dbPassword = (String) resolve(conf.field("dbPassword"));

    if (conf.containsField("serverUser"))
      serverUser = (String) resolve(conf.field("serverUser"));
    if (conf.containsField("serverPassword"))
      serverPassword = (String) resolve(conf.field("serverPassword"));

    if (conf.containsField("dbType"))
      dbType = DB_TYPE.valueOf(conf.field("dbType").toString().toUpperCase());
    if (conf.containsField("tx"))
      tx = conf.<Boolean>field("tx");
    if (conf.containsField("wal"))
      wal = conf.<Boolean>field("wal");
    if (conf.containsField("txUseLog"))
      txUseLog = conf.<Boolean>field("txUseLog");
    if (conf.containsField("batchCommit"))
      batchCommitSize = conf.<Integer>field("batchCommit");
    if (conf.containsField("dbAutoCreate"))
      dbAutoCreate = conf.<Boolean>field("dbAutoCreate");
    if (conf.containsField("dbAutoDropIfExists"))
      dbAutoDropIfExists = conf.<Boolean>field("dbAutoDropIfExists");
    if (conf.containsField("dbAutoCreateProperties"))
      dbAutoCreateProperties = conf.<Boolean>field("dbAutoCreateProperties");
    if (conf.containsField("useLightweightEdges"))
      useLightweightEdges = conf.<Boolean>field("useLightweightEdges");
    if (conf.containsField("standardElementConstraints"))
      standardElementConstraints = conf.<Boolean>field("standardElementConstraints");

    clusterName = conf.field("cluster");
    className = conf.field("class");
    indexes = conf.field("indexes");
    classes = conf.field("classes");

    if (conf.containsField("settings")) {
      final ODocument settings = conf.field("settings");
      settings.setAllowChainedAccess(false);
      for (String s : settings.fieldNames()) {
        final OGlobalConfiguration v = OGlobalConfiguration.findByKey(s);
        if (v != null)
          v.setValue(settings.field(s));
      }
    }

    // use wal or not
    OGlobalConfiguration.USE_WAL.setValue(wal);

    if (dbURL.startsWith("remote")) {

      manageRemoteDatabase();

    } else {

      switch (dbType) {
      case DOCUMENT:
        configureDocumentDB();
        break;

      case GRAPH:
        configureGraphDB();
        break;
      }
    }
  }

  private void configureGraphDB() {
    final OrientGraphFactory factory = new OrientGraphFactory(dbURL, dbUser, dbPassword);

    if (dbAutoDropIfExists && factory.exists()) {
      log(INFO, "Dropping existent database '%s'...", dbURL);
      factory.drop();
    }

    final OrientBaseGraph graphDatabase = tx ? factory.getTx() : factory.getNoTx();
    graphDatabase.shutdown();
  }

  private void configureDocumentDB() {
    final ODatabaseDocument documentDatabase = new ODatabaseDocumentTx(dbURL);

    if (documentDatabase.exists() && dbAutoDropIfExists) {
      log(INFO, "Dropping existent database '%s'...", dbURL);
      documentDatabase.open(dbUser, dbPassword);
      documentDatabase.drop();
    }

    if (documentDatabase.exists()) {
      log(INFO, "Opening database '%s'...", dbURL);
      documentDatabase.open(dbUser, dbPassword);
    } else if (dbAutoCreate) {
      documentDatabase.create();
    } else {
      throw new IllegalArgumentException("Database '" + dbURL + "' not exists and 'dbAutoCreate' setting is false");
    }
    documentDatabase.close();
  }

  private void manageRemoteDatabase() {
    if (!dbAutoCreate && !dbAutoDropIfExists) {
      log(INFO, "nothing setup  on remote database " + dbURL);
      return;
    }

    if (NOT_DEF.equals(serverPassword) || NOT_DEF.equals(serverUser)) {
      log(ERROR, "please provide server administrator credentials");
      throw new OLoaderException("unable to manage remote db without server admin credentials");
    }

    ODatabaseDocument documentDatabase = new ODatabaseDocumentTx(dbURL);
    try {
      OServerAdmin admin = new OServerAdmin(documentDatabase.getURL()).connect(serverUser, serverPassword);
      boolean exists = admin.existsDatabase();
      if (!exists && dbAutoCreate) {
        admin.createDatabase(documentDatabase.getName(), dbType.name(), "plocal");
      } else if (exists && dbAutoDropIfExists) {
        admin.dropDatabase(documentDatabase.getName(), documentDatabase.getType());
      }
      admin.close();

    } catch (IOException e) {
      throw new OLoaderException(e);
    }
    documentDatabase.close();
  }

  @Override
  public void begin() {

  }

  public void beginLoader(OETLPipeline pipeline) {

    synchronized (this) {
      ODatabaseDocument documentDatabase = null;
      OrientBaseGraph graphDatabase = null;

      final OrientGraphFactory factory = new OrientGraphFactory(dbURL, dbUser, dbPassword);

      graphDatabase = tx ? factory.getTx() : factory.getNoTx();

      graphDatabase.setUseLightweightEdges(useLightweightEdges);
      graphDatabase.setStandardElementConstraints(standardElementConstraints);

      documentDatabase = graphDatabase.getRawGraph();

      pipeline.setDocumentDatabase(documentDatabase);
      pipeline.setGraphDatabase(graphDatabase);

      createSchema(pipeline, documentDatabase);

      documentDatabase.getMetadata().getSchema().reload();

      documentDatabase.declareIntent(new OIntentMassiveInsert());
    }

  }

  private void createSchema(OETLPipeline pipeline, ODatabaseDocument documentDatabase) {
    if (classes != null) {
      for (ODocument cls : classes) {
        schemaClass = getOrCreateClass(pipeline, (String) cls.field("name"), (String) cls.field("extends"));

        Integer clusters = cls.field("clusters");
        if (clusters != null)
          OClassImpl.addClusters(schemaClass, clusters);

        log(DEBUG, "%s: found %d %s in class '%s'", getName(), schemaClass.count(), getUnit(), className);
      }
    }
    if (className != null) {
      schemaClass = getOrCreateClass(pipeline, className, null);
      log(DEBUG, "%s: found %d %s in class '%s'", getName(), schemaClass.count(), getUnit(), className);
    }
    if (indexes != null) {
      for (ODocument idx : indexes) {
        OIndex index;

        final ODocument metadata = (ODocument) resolve(idx.field("metadata"));
        log(DEBUG, "%s: found metadata field '%s'", getName(), metadata);

        String idxName = (String) resolve(idx.field("name"));
        if (idxName != null) {
          index = documentDatabase.getMetadata().getIndexManager().getIndex(idxName);
          if (index != null)
            // ALREADY EXISTS
            continue;
        }

        final String idxClass = (String) resolve(idx.field("class"));
        if (idxClass == null)
          throw new OConfigurationException("Index 'class' missed in OrientDB Loader");

        final OClass cls = getOrCreateClass(pipeline, idxClass, null);
        final String idxType = idx.field("type");
        if (idxType == null)
          throw new OConfigurationException("Index 'type' missed in OrientDB Loader for index '" + idxName + "'");

        final List<String> idxFields = idx.field("fields");
        if (idxFields == null)
          throw new OConfigurationException("Index 'fields' missed in OrientDB Loader");

        String[] fields = new String[idxFields.size()];
        for (int f = 0; f < fields.length; ++f) {
          final String fieldName = idxFields.get(f);

          final String[] fieldNameParts = fieldName.split(":");

          if (!cls.existsProperty(fieldNameParts[0])) {
            // CREATE PROPERTY AUTOMATICALLY

            if (fieldNameParts.length < 2)
              throw new OConfigurationException("Index field type missed in OrientDB Loader for field '" + fieldName + "'");

            final String fieldType = fieldNameParts[1].toUpperCase();
            final OType type = OType.valueOf(fieldType);

            cls.createProperty(fieldNameParts[0], type);
            log(DEBUG, "- OrientDBLoader: created property '%s.%s' of type: %s", idxClass, fieldNameParts[0], fieldNameParts[1]);
          }

          fields[f] = fieldNameParts[0];
        }

        if (idxName == null) {
          // CREATE INDEX NAME
          idxName = idxClass + ".";
          for (int i = 0; i < fields.length; ++i) {
            if (i > 0)
              idxName += '_';
            idxName += fields[i];
          }
        }

        index = documentDatabase.getMetadata().getIndexManager().getIndex(idxName);
        if (index != null)
          // ALREADY EXISTS
          continue;

        index = cls.createIndex(idxName, idxType, null, metadata, fields);
        log(DEBUG, "- OrientDocumentLoader: created index '%s' type '%s' against Class '%s', fields %s", idxName, idxType, idxClass,
            idxFields);
      }
    }
  }

  @Override
  public void endLoader(OETLPipeline pipeline) {
    log(INFO, "committing");
    if (dbType == DOCUMENT)
      pipeline.getDocumentDatabase().commit();
    else
      pipeline.getGraphDatabase().commit();
  }

  @Override
  public void end() {
  }

  @Override
  public String getName() {
    return "orientdb";
  }

  protected enum DB_TYPE {
    DOCUMENT, GRAPH
  }
}
