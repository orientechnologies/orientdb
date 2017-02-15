/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (Level.INFO(-at-)orientdb.com)
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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.etl.OETLPipeline;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.orientechnologies.orient.etl.loader.OETLOrientDBLoader.DB_TYPE.DOCUMENT;
import static com.orientechnologies.orient.etl.loader.OETLOrientDBLoader.DB_TYPE.GRAPH;

/**
 * ETL Loader that saves record into OrientDB database.
 */
public class OETLOrientDBLoader extends OETLAbstractLoader implements OETLLoader {

  private static String NOT_DEF = "not_defined";
  public  ODatabasePool   pool;
  public  OrientDB        orient;
  private String          clusterName;
  private String          className;
  private List<ODocument> classes;
  private List<ODocument> indexes;
  private OClass          schemaClass;
  private String          dbURL;
  private String     dbUser                     = "admin";
  private String     dbPassword                 = "admin";
  private String     serverUser                 = NOT_DEF;
  private String     serverPassword             = NOT_DEF;
  private boolean    dbAutoCreate               = true;
  private boolean    dbAutoDropIfExists         = false;
  private boolean    dbAutoCreateProperties     = false;
  private boolean    useLightweightEdges        = false;
  private boolean    standardElementConstraints = true;
  private boolean    tx                         = false;
  private int        batchCommitSize            = 0;
  private AtomicLong batchCounter               = new AtomicLong(0);
  private DB_TYPE    dbType                     = DOCUMENT;
  private boolean    wal                        = true;
  private boolean    txUseLog                   = false;
  private boolean    skipDuplicates             = false;

  public OETLOrientDBLoader() {
  }

  public ODatabasePool getPool() {
    return pool;
  }

  @Override
  public void load(ODatabaseDocument db, final Object input, OCommandContext context) {

    if (input == null)
      return;

    if (dbAutoCreateProperties) {
      autoCreateProperties(db, input);
    }

    if (tx) {
      if (!db.getTransaction().isActive()) {
        db.begin();
        db.getTransaction().setUsingLog(txUseLog);
      }
    }

    if (input instanceof OVertex) {
      final OVertex v = (OVertex) input;

      log(Level.INFO, "v::" + v.getSchemaType().get());
      try {
        v.save(clusterName);
      } catch (ORecordDuplicatedException e) {
        if (skipDuplicates) {
        } else {
          throw e;
        }
      } finally {

      }
    } else if (input instanceof ODocument) {

      final ODocument doc = (ODocument) input;

      if (className != null) {
        doc.setClassName(className);
      }

      if (clusterName != null) {
        db.save(doc, clusterName);
      } else {
        db.save(doc);
      }

    } else {
      OLogManager.instance().error(this, "input type not supported::  %s", input.getClass());
    }

    progress.incrementAndGet();

    // DO BATCH COMMIT if on TX

    if (tx && batchCommitSize > 0 && batchCounter.get() > batchCommitSize) {
      synchronized (this) {
        if (batchCommitSize > 0 && batchCounter.get() > batchCommitSize) {
          log(Level.FINE, "committing document batch %d", progress.get());
          db.commit();
          db.begin();
          db.getTransaction().setUsingLog(txUseLog);
          batchCounter.set(0);
        }
      }
    } else {
      batchCounter.incrementAndGet();
    }
  }

  private void autoCreateProperties(ODatabaseDocument db, Object input) {
    if (dbType == DOCUMENT && input instanceof ODocument) {
      autoCreatePropertiesOnDocument(db, (ODocument) input);
    } else if (dbType == GRAPH && input instanceof OVertex) {
      autoCreatePropertiesOnElement(db, (OVertex) input);
    }
  }

  private void autoCreatePropertiesOnElement(ODatabaseDocument db, OVertex element) {

    final OClass cls;
    Optional<OClass> schemaType = element.getSchemaType();
    final String clsName = schemaType.get().getName();

    if (clsName != null)
      cls = getOrCreateClass(db, clsName, element.getSchemaType().get().getName());
    else
      throw new IllegalArgumentException("No class defined on graph element: " + element);

    for (String f : element.getPropertyNames()) {
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

  private void autoCreatePropertiesOnDocument(ODatabaseDocument db, ODocument doc) {
    final OClass cls;
    if (className != null)
      cls = getOrCreateClass(db, className, null);
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
  public void rollback(ODatabaseDocument db) {
    if (tx) {
      if (db.getTransaction().isActive())
        db.rollback();
    }
  }

  protected OClass getOrCreateClass(ODatabaseDocument db, final String iClassName, final String iSuperClass) {
    OClass cls;

    if (dbType == DOCUMENT) {
      cls = getOrCreateClassOnDocument(db, iClassName, iSuperClass);

    } else {
      cls = getOrCreateClassOnGraph(db, iClassName, iSuperClass);

    }

    db.activateOnCurrentThread();
    if (clusterName != null) {
      int clusterIdByName = db.getClusterIdByName(clusterName);
      if (clusterIdByName == -1) {
        log(logLevel, "add cluster :: " + clusterName);
        cls.addCluster(clusterName);
      }
    }
    return cls;
  }

  private OClass getOrCreateClassOnGraph(ODatabaseDocument db, String iClassName, String iSuperClass) {
    OClass cls;// GRAPH
    OSchema schema = db.getMetadata().getSchema();
    cls = schema.getClass(iClassName);

    if (cls == null) {

      if (iSuperClass != null) {
        final OClass superClass = schema.getClass(iSuperClass);
        if (superClass == null)
          throw new OETLLoaderException("Cannot find super class '" + iSuperClass + "'");

        OSchema schema1 = db.getMetadata().getSchema();

        if (schema1.getClass("V").isSuperClassOf(superClass)) {
          // VERTEX

          cls = db.createVertexClass(iClassName).setSuperClasses(Arrays.asList(superClass));
//          cls = schema1.createClass(iClassName, superClass);
          log(Level.FINE, "- OrientDBLoader: created vertex class '%s' extends '%s'", iClassName, iSuperClass);
        } else {
          // EDGE
          cls = db.createEdgeClass(iClassName).setSuperClasses(Arrays.asList(superClass));

//          cls = graphDatabase.createEdgeType(iClassName, superClass);
          log(Level.FINE, "- OrientDBLoader: created edge class '%s' extends '%s'", iClassName, iSuperClass);
        }
      } else {
        // ALWAYS CREATE SUB-VERTEX
        cls = db.createVertexClass(iClassName);
//        cls = graphDatabase.createVertexType(iClassName);
        log(Level.FINE, "- OrientDBLoader: created vertex class '%s'", iClassName);
      }
    }
    return cls;
  }

  private OClass getOrCreateClassOnDocument(ODatabaseDocument documentDatabase, String iClassName, String iSuperClass) {
    OClass cls;// DOCUMENT
    if (documentDatabase.getMetadata().getSchema().existsClass(iClassName))
      cls = documentDatabase.getMetadata().getSchema().getClass(iClassName);
    else {
      if (iSuperClass != null) {
        final OClass superClass = documentDatabase.getMetadata().getSchema().getClass(iSuperClass);
        if (superClass == null)
          throw new OETLLoaderException("Cannot find super class '" + iSuperClass + "'");

        cls = documentDatabase.getMetadata().getSchema().createClass(iClassName, superClass);
        log(Level.FINE, "- OrientDBLoader: created class '%s' extends '%s'", iClassName, iSuperClass);
      } else {
        cls = documentDatabase.getMetadata().getSchema().createClass(iClassName);
        log(Level.FINE, "- OrientDBLoader: created class '%s'", iClassName);
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

      log(Level.FINE, "created property [%s.%s] of type [%s]", cls.getName(), f, fType);
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
  public void configure(final ODocument conf, final OCommandContext iContext) {
    super.configure(conf, iContext);

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

    if (conf.containsField("skipDuplicates"))
      skipDuplicates = conf.field("skipDuplicates");

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

    createDatabasePool();
    if (dbURL.startsWith("remote")) {

      manageRemoteDatabase();

    } else {

      configureDocumentDB();
    }
  }

  private void configureDocumentDB() {
    ODatabaseDocument documentDatabase = pool.acquire();

//    if (documentDatabase.exists() && dbAutoDropIfExists) {
//      log(Level.INFO, "Dropping existent database '%s'...", dbURL);
//      documentDatabase.open(dbUser, dbPassword);
//      documentDatabase.drop();
//    }
//
//    if (documentDatabase.exists()) {
//      log(Level.INFO, "Opening database '%s'...", dbURL);
//      documentDatabase.open(dbUser, dbPassword);
//    } else if (dbAutoCreate) {
//      documentDatabase.create();
//    } else {
//      throw new IllegalArgumentException("Database '" + dbURL + "' not exists and 'dbAutoCreate' setting is false");
//    }
    documentDatabase.close();
  }

  private void manageRemoteDatabase() {
    if (!dbAutoCreate && !dbAutoDropIfExists) {
      log(Level.INFO, "nothing setup  on remote database " + dbURL);
      return;
    }

    if (NOT_DEF.equals(serverPassword) || NOT_DEF.equals(serverUser)) {
      log(Level.SEVERE, "please provide server administrator credentials");
      throw new OETLLoaderException("unable to manage remote db without server admin credentials");
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
      throw new OETLLoaderException(e);
    }
    documentDatabase.close();
  }

  @Override
  public void begin(ODatabaseDocument db) {

  }

  @Override
  public synchronized void beginLoader(OETLPipeline pipeline) {
    ODatabaseDocument db = pool.acquire();
    db.activateOnCurrentThread();
    createSchema(db);
    db.close();
    pipeline.setPool(pool);
  }

  private void createDatabasePool() {
    if (pool != null)
      return;

    String kind = dbURL.substring(0, dbURL.indexOf(":"));
    String dbCtx = dbURL.substring(dbURL.indexOf(":") + 1);

    if ("memory".equalsIgnoreCase(kind)) {

      orient = new OrientDB("embedded:" + dbCtx, dbUser, dbPassword, null);
      if (orient.exists(dbCtx) && dbAutoDropIfExists) {
        orient.drop(dbCtx);
      }

      if (!orient.exists(dbCtx)) {
        orient.create(dbCtx, ODatabaseType.MEMORY);
      }

      pool = new ODatabasePool(orient, dbCtx, dbUser, dbPassword);
    } else if ("plocal".equalsIgnoreCase(kind)) {

      String dbName = dbCtx.substring(dbCtx.lastIndexOf("/"));
      orient = new OrientDB("embedded:" + dbCtx, dbUser, dbPassword, null);

      if (orient.exists(dbName) && dbAutoDropIfExists) {
        orient.drop(dbName);
      }

      if (!orient.exists(dbName) && dbAutoCreate) {
        orient.create(dbName, ODatabaseType.PLOCAL);
      }
      pool = new ODatabasePool(orient, dbName, dbUser, dbPassword);
    } else {
      orient = new OrientDB("remote:" + dbCtx, dbUser, dbPassword, null);
      String dbName = dbCtx.substring(dbCtx.lastIndexOf("/"));
      if (orient.exists(dbName) && dbAutoDropIfExists) {
        orient.drop(dbName);
      }

      if (!orient.exists(dbName) && dbAutoCreate) {
        orient.create(dbName, ODatabaseType.PLOCAL);
      }
      pool = new ODatabasePool(orient, dbName, dbUser, dbPassword);

    }

  }

  private void createSchema(ODatabaseDocument db) {

    if (classes != null) {
      for (ODocument cls : classes) {
        schemaClass = getOrCreateClass(db, cls.field("name"), cls.field("extends"));

        Integer clusters = cls.field("clusters");
        if (clusters != null)
          OClassImpl.addClusters(schemaClass, clusters);

        log(Level.FINE, "%s: found %d %s in class '%s'", getName(), schemaClass.count(), getUnit(), className);
      }
    }
    if (className != null) {
      schemaClass = getOrCreateClass(db, className, null);
      log(Level.FINE, "%s: found %d %s in class '%s'", getName(), schemaClass.count(), getUnit(), className);
    }
    if (indexes != null) {
      for (ODocument idx : indexes) {
        OIndex index;

        final ODocument metadata = (ODocument) resolve(idx.field("metadata"));
        log(Level.FINE, "%s: found metadata field '%s'", getName(), metadata);

        String idxName = (String) resolve(idx.field("name"));
        if (idxName != null) {
          index = db.getMetadata().getIndexManager().getIndex(idxName);
          if (index != null)
            // ALREADY EXISTS
            continue;
        }

        final String idxClass = (String) resolve(idx.field("class"));
        if (idxClass == null)
          throw new OConfigurationException("Index 'class' missed in OrientDB Loader");

        final OClass cls = getOrCreateClass(db, idxClass, null);
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
            log(Level.FINE, "- OrientDBLoader: created property '%s.%s' of type: %s", idxClass, fieldNameParts[0],
                fieldNameParts[1]);
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

        index = db.getMetadata().getIndexManager().getIndex(idxName);
        if (index != null)
          // ALREADY EXISTS
          continue;

        index = cls.createIndex(idxName, idxType, null, metadata, fields);
        log(Level.FINE, "- OrientDocumentLoader: created index '%s' type '%s' against Class '%s', fields %s", idxName, idxType,
            idxClass, idxFields);
      }
    }
//    db.activateOnCurrentThread();
//    db.close();
  }

  @Override
  public void end() {
  }

  @Override
  public void close() {
    orient.close();
  }

  @Override
  public String getName() {
    return "orientdb";
  }

  protected enum DB_TYPE {
    DOCUMENT, GRAPH
  }
}
