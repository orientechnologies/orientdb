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

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.List;

/**
 * ETL Loader that saves record into OrientDB database.
 */
public class OOrientDBLoader extends OAbstractLoader implements OLoader {
  protected String          clusterName;
  protected String          className;
  protected List<ODocument> classes;
  protected List<ODocument> indexes;
  protected OClass          schemaClass;
  protected String          dbURL;
  protected String          dbUser                 = "admin";
  protected String          dbPassword             = "admin";
  protected boolean         dbAutoCreate           = true;
  protected boolean         dbAutoDropIfExists     = false;
  protected boolean         dbAutoCreateProperties = false;
  protected boolean         tx                     = false;
  protected int             batchCommit            = 0;
  protected long            batchCounter           = 0;
  protected DB_TYPE         dbType                 = DB_TYPE.DOCUMENT;
  protected boolean         wal                    = true;

  protected enum DB_TYPE {
    DOCUMENT, GRAPH
  }

  public OOrientDBLoader() {
  }

  public void load(final Object input, OCommandContext context) {
    if (input == null)
      return;

    if (dbAutoCreateProperties) {
      if (dbType == DB_TYPE.DOCUMENT) {
        if (input instanceof ODocument) {
          final ODocument doc = (ODocument) input;
          final ODatabaseDocumentTx documentDatabase = pipeline.getDocumentDatabase();
          final OClass cls;
          if (className != null)
            cls = getOrCreateClass(className, null);
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
      } else if (dbType == DB_TYPE.GRAPH) {
        if (input instanceof OrientElement) {
          final OrientElement element = (OrientElement) input;

          final OClass cls;
          final String clsName = className != null ? className : (element instanceof OrientVertex ? ((OrientVertex) element)
              .getLabel() : ((OrientEdge) element).getLabel());
          if (clsName != null)
            cls = getOrCreateClass(clsName, element.getBaseClassName());
          else
            throw new IllegalArgumentException("No class defined on graph element: " + input);

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
      }
    }

    if (tx && dbType == DB_TYPE.DOCUMENT) {
      final ODatabaseDocumentTx documentDatabase = pipeline.getDocumentDatabase();
      if (!documentDatabase.getTransaction().isActive())
        // BEGIN THE TRANSACTION FIRST
        documentDatabase.begin();
    }

    if (input instanceof OrientVertex) {
      // VERTEX
      final OrientVertex v = (OrientVertex) input;

      if (clusterName != null)
        // SAVE INTO THE CUSTOM CLUSTER
        v.save(clusterName);
      else
        // SAVE INTO THE DEFAULT CLUSTER
        v.save();

    } else if (input instanceof ODocument) {
      // DOCUMENT
      final ODocument doc = (ODocument) input;

      if (className != null)
        doc.setClassName(className);

      if (clusterName != null)
        // SAVE INTO THE CUSTOM CLUSTER
        doc.save(clusterName);
      else
        // SAVE INTO THE DEFAULT CLUSTER
        doc.save();
    }

    progress.incrementAndGet();

    if (batchCommit > 0) {
      if (batchCounter > batchCommit) {
        if (dbType == DB_TYPE.DOCUMENT) {
          final ODatabaseDocumentTx documentDatabase = pipeline.getDocumentDatabase();
          documentDatabase.commit();
          documentDatabase.begin();
        } else {
          pipeline.getGraphDatabase().commit();
        }
        batchCounter = 0;
      } else
        batchCounter++;
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
        + "{wal:{optional:true,description:'Use the WAL (Write Ahead Log)'}},"
        + "{wal:{optional:true,description:'Use the WAL (Write Ahead Log)'}},"
        + "{cluster:{optional:true,description:'Cluster name where to store the new record'}},"
        + "{classes:{optional:true,description:'Classes used. It assure the classes exist or in case create them'}},"
        + "{indexes:{optional:true,description:'Indexes used. It assure the indexes exist or in case create them'}}],"
        + "input:['OrientVertex','ODocument']}");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    if (iConfiguration.containsField("dbURL"))
      dbURL = iConfiguration.field("dbURL");
    if (iConfiguration.containsField("dbUser"))
      dbUser = iConfiguration.field("dbUser");
    if (iConfiguration.containsField("dbPassword"))
      dbPassword = iConfiguration.field("dbPassword");
    if (iConfiguration.containsField("dbType"))
      dbType = DB_TYPE.valueOf(iConfiguration.field("dbType").toString().toUpperCase());
    if (iConfiguration.containsField("tx"))
      tx = (Boolean) iConfiguration.field("tx");
    if (iConfiguration.containsField("wal"))
      wal = (Boolean) iConfiguration.field("wal");
    if (iConfiguration.containsField("batchCommit"))
      batchCommit = (Integer) iConfiguration.field("batchCommit");
    if (iConfiguration.containsField("dbAutoCreate"))
      dbAutoCreate = (Boolean) iConfiguration.field("dbAutoCreate");
    if (iConfiguration.containsField("dbAutoDropIfExists"))
      dbAutoDropIfExists = (Boolean) iConfiguration.field("dbAutoDropIfExists");
    if (iConfiguration.containsField("dbAutoCreateProperties"))
      dbAutoCreateProperties = (Boolean) iConfiguration.field("dbAutoCreateProperties");

    clusterName = iConfiguration.field("cluster");
    className = iConfiguration.field("class");
    indexes = iConfiguration.field("indexes");
    classes = iConfiguration.field("classes");

    switch (dbType) {
    case DOCUMENT:
      final ODatabaseDocumentTx documentDatabase = new ODatabaseDocumentTx(dbURL);
      if (documentDatabase.exists() && dbAutoDropIfExists) {
        log(OETLProcessor.LOG_LEVELS.INFO, "Dropping existent database '%s'...", dbURL);
        documentDatabase.open(dbUser, dbPassword);
        documentDatabase.drop();
      }

      if (documentDatabase.exists()) {
        log(OETLProcessor.LOG_LEVELS.DEBUG, "Opening database '%s'...", dbURL);
        documentDatabase.open(dbUser, dbPassword);
      } else if (dbAutoCreate) {
        documentDatabase.create();
      } else
        throw new IllegalArgumentException("Database '" + dbURL + "' not exists and 'dbAutoCreate' setting is false");

      documentDatabase.close();
      break;

    case GRAPH:
      final OrientGraphFactory factory = new OrientGraphFactory(dbURL);
      if (dbAutoDropIfExists && factory.exists()) {
        log(OETLProcessor.LOG_LEVELS.INFO, "Dropping existent database '%s'...", dbURL);
        factory.drop();
      }

      final OrientBaseGraph graphDatabase = tx ? factory.getTx() : factory.getNoTx();
      graphDatabase.shutdown();
      break;
    }
  }

  @Override
  public void begin() {
    if (!wal)
      OGlobalConfiguration.USE_WAL.setValue(wal);

    ODatabaseDocumentTx documentDatabase = init();

    if (documentDatabase == null) {
      switch (dbType) {
      case DOCUMENT:
        documentDatabase = new ODatabaseDocumentTx(dbURL);
        documentDatabase.open(dbUser, dbPassword);
        break;

      case GRAPH:
        final OrientGraphFactory factory = new OrientGraphFactory(dbURL);
        final OrientBaseGraph graphDatabase = tx ? factory.getTx() : factory.getNoTx();
        documentDatabase = graphDatabase.getRawGraph();

        pipeline.setGraphDatabase(graphDatabase);
        break;
      }
      pipeline.setDocumentDatabase(documentDatabase);
    }
    documentDatabase.declareIntent(new OIntentMassiveInsert());
  }

  @Override
  public void end() {
    if (dbType == DB_TYPE.DOCUMENT)
      pipeline.getDocumentDatabase().commit();
    else
      pipeline.getGraphDatabase().commit();
  }

  @Override
  public String getName() {
    return "orientdb";
  }

  public String getUnit() {
    return dbType == DB_TYPE.DOCUMENT ? "documents" : "vertices";
  }

  @Override
  public void rollback() {
    if (tx)
      if (dbType == DB_TYPE.DOCUMENT) {
        final ODatabaseDocumentTx documentDatabase = pipeline.getDocumentDatabase();
        if (documentDatabase.getTransaction().isActive())
          documentDatabase.rollback();
      } else
        pipeline.getGraphDatabase().rollback();
  }

  protected void createProperty(final OClass cls, final String f, final Object fValue) {
    if (fValue != null) {
      final OType fType = OType.getTypeByClass(fValue.getClass());

      try {
        cls.createProperty(f, fType);
      } catch (OSchemaException e) {
      }

      log(OETLProcessor.LOG_LEVELS.DEBUG, "created property [%s.%s] of type [%s]", cls.getName(), f, fType);
    }
  }

  protected synchronized ODatabaseDocumentTx init() {
    ODatabaseDocumentTx documentDatabase = pipeline.getDocumentDatabase();
    OrientBaseGraph graphDatabase;

    if (documentDatabase == null) {
      switch (dbType) {
      case DOCUMENT:
        documentDatabase = new ODatabaseDocumentTx(dbURL);
        documentDatabase.open(dbUser, dbPassword);
        break;

      case GRAPH:
        final OrientGraphFactory factory = new OrientGraphFactory(dbURL);
        graphDatabase = factory.getNoTx();
        documentDatabase = graphDatabase.getRawGraph();
        pipeline.setGraphDatabase(graphDatabase);
        break;
      }
      pipeline.setDocumentDatabase(documentDatabase);
    } else
      ODatabaseRecordThreadLocal.INSTANCE.set(documentDatabase);

    if (className != null) {
      schemaClass = getOrCreateClass(className, null);
      log(OETLProcessor.LOG_LEVELS.DEBUG, "%s: found %d %s in class '%s'", getName(), schemaClass.count(), getUnit(), className);
    }

    if (classes != null) {
      for (ODocument cls : classes) {
        schemaClass = getOrCreateClass((String) cls.field("name"), (String) cls.field("extends"));
        log(OETLProcessor.LOG_LEVELS.DEBUG, "%s: found %d %s in class '%s'", getName(), schemaClass.count(), getUnit(), className);
      }
    }

    if (indexes != null) {
      for (ODocument idx : indexes) {
        OIndex index;

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

        final OClass cls = getOrCreateClass(idxClass, null);
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
            log(OETLProcessor.LOG_LEVELS.DEBUG, "- OrientDBLoader: created property '%s.%s' of type: %s", idxClass,
                fieldNameParts[0], fieldNameParts[1]);
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

        index = cls.createIndex(idxName, idxType, fields);
        log(OETLProcessor.LOG_LEVELS.DEBUG, "- OrientDocumentLoader: created index '%s' type '%s' against Class '%s', fields %s",
            idxName, idxType, idxClass, idxFields);
      }
    }
    return documentDatabase;
  }

  protected OClass getOrCreateClass(final String iClassName, final String iSuperClass) {
    OClass cls;

    if (dbType == DB_TYPE.DOCUMENT) {
      // DOCUMENT
      final ODatabaseDocumentTx documentDatabase = pipeline.getDocumentDatabase();
      if (documentDatabase.getMetadata().getSchema().existsClass(iClassName))
        cls = documentDatabase.getMetadata().getSchema().getClass(iClassName);
      else {
        if (iSuperClass != null) {
          final OClass superClass = documentDatabase.getMetadata().getSchema().getClass(iSuperClass);
          if (superClass == null)
            throw new OLoaderException("Cannot find super class '" + iSuperClass + "'");

          cls = documentDatabase.getMetadata().getSchema().createClass(iClassName, superClass);
          log(OETLProcessor.LOG_LEVELS.DEBUG, "- OrientDBLoader: created class '%s' extends '%s'", iClassName, iSuperClass);
        } else {
          cls = documentDatabase.getMetadata().getSchema().createClass(iClassName);
          log(OETLProcessor.LOG_LEVELS.DEBUG, "- OrientDBLoader: created class '%s'", iClassName);
        }
      }
    } else {
      // GRAPH
      final OrientBaseGraph graphDatabase = pipeline.getGraphDatabase();
      cls = graphDatabase.getVertexType(iClassName);
      if (cls == null) {

        if (iSuperClass != null) {
          final OClass superClass = graphDatabase.getRawGraph().getMetadata().getSchema().getClass(iSuperClass);
          if (superClass == null)
            throw new OLoaderException("Cannot find super class '" + iSuperClass + "'");

          if (graphDatabase.getVertexBaseType().isSuperClassOf(superClass)) {
            // VERTEX
            cls = graphDatabase.createVertexType(iClassName, superClass);
            log(OETLProcessor.LOG_LEVELS.DEBUG, "- OrientDBLoader: created vertex class '%s' extends '%s'", iClassName, iSuperClass);
          } else {
            // EDGE
            cls = graphDatabase.createEdgeType(iClassName, superClass);
            log(OETLProcessor.LOG_LEVELS.DEBUG, "- OrientDBLoader: created edge class '%s' extends '%s'", iClassName, iSuperClass);
          }
        } else {
          // ALWAYS CREATE SUB-VERTEX
          cls = graphDatabase.createVertexType(iClassName);
          log(OETLProcessor.LOG_LEVELS.DEBUG, "- OrientDBLoader: created vertex class '%s'", iClassName);
        }
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
}
