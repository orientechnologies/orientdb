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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OAbstractETLComponent;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.List;

/**
 * ETL Loader.
 */
public class OOrientDBLoader extends OAbstractETLComponent implements OLoader {
  protected long                progress     = 0;
  protected String              clusterName;
  protected String              className;
  protected List<ODocument>     indexes;
  protected OClass              schemaClass;
  protected String              dbURL;
  protected String              dbUser       = "admin";
  protected String              dbPassword   = "admin";
  protected boolean             dbAutoCreate = true;
  protected boolean             tx           = true;
  protected int                 batchCommit  = 0;
  protected long                batchCounter = 0;
  protected DB_TYPE             dbType       = DB_TYPE.DOCUMENT;
  protected ODatabaseDocumentTx documentDatabase;
  protected OrientBaseGraph     graphDatabase;

  protected enum DB_TYPE {
    DOCUMENT, GRAPH
  }

  public OOrientDBLoader() {
  }

  public void load(final Object input, OCommandContext context) {
    if (input == null)
      return;

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

    progress++;

    if (batchCommit > 0) {
      if (batchCounter > batchCommit) {
        if (dbType == DB_TYPE.DOCUMENT) {
          documentDatabase.commit();
          documentDatabase.begin();
        } else {
          graphDatabase.commit();
        }
      }
      batchCounter++;
    }
  }

  public long getProgress() {
    return progress;
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + "{dbUrl:{optional:false,description:'Database URL'}},"
        + "{dbUser:{optional:true,description:'Database user, default is admin'}},"
        + "{dbPassword:{optional:true,description:'Database password, default is admin'}},"
        + "{dbType:{optional:true,description:'Database type, default is document',values:" + stringArray2Json(DB_TYPE.values())
        + "}}," + "{class:{optional:true,description:'Record class name'}},"
        + "{cluster:{optional:true,description:'Cluster name where to store the new record'}},"
        + "{indexes:{optional:true,description:'Index used. It assure the index exist or in case create them'}}],"
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
      tx = iConfiguration.field("tx");
    if (iConfiguration.containsField("batchCommit"))
      batchCommit = iConfiguration.field("batchCommit");

    clusterName = iConfiguration.field("cluster");
    className = iConfiguration.field("class");
    indexes = iConfiguration.field("indexes");

    switch (dbType) {
    case DOCUMENT:
      documentDatabase = new ODatabaseDocumentTx(dbURL);
      if (documentDatabase.exists()) {
        documentDatabase.open(dbUser, dbPassword);
      } else {
        if (dbAutoCreate) {
          documentDatabase.create();
        } else {
          throw new IllegalArgumentException("Database '" + dbURL + "' not exists and 'dbAutoCreate' setting is false");
        }
      }
      break;

    case GRAPH:
      graphDatabase = new OrientGraphFactory(dbURL).setTransactional(tx).get();
      documentDatabase = graphDatabase.getRawGraph();
      break;
    }
  }

  @Override
  public void begin() {
    if (className != null) {
      schemaClass = getOrCreateClass(className);
      processor.out(true, "%s: found %d %s in class '%s'", getName(), schemaClass.count(), getUnit(), className);
    }

    if (indexes != null) {
      for (ODocument idx : indexes) {
        OIndex index;

        String idxName = idx.field("name");
        if (idxName != null) {
          index = documentDatabase.getMetadata().getIndexManager().getIndex(idxName);
          if (index != null)
            // ALREADY EXISTS
            continue;
        }

        final String idxClass = idx.field("class");
        if (idxClass == null)
          throw new OConfigurationException("Index 'class' missed in OrientDocument loader");

        final OClass cls = getOrCreateClass(idxClass);
        final String idxType = idx.field("type");
        if (idxType == null)
          throw new OConfigurationException("Index 'type' missed in OrientDocument loader for index '" + idxName + "'");

        final List<String> idxFields = idx.field("fields");
        if (idxFields == null)
          throw new OConfigurationException("Index 'fields' missed in OrientDocument loader");

        String[] fields = new String[idxFields.size()];
        for (int f = 0; f < fields.length; ++f) {
          final String fieldName = idxFields.get(f);

          final String[] fieldNameParts = fieldName.split(":");

          if (!cls.existsProperty(fieldNameParts[0])) {
            // CREATE PROPERTY AUTOMATICALLY

            if (fieldNameParts.length < 2)
              throw new OConfigurationException("Index field type missed in OrientDocument loader for field '" + fieldName + "'");

            final String fieldType = fieldNameParts[1].toUpperCase();
            final OType type = OType.valueOf(fieldType);

            cls.createProperty(fieldNameParts[0], type);
            processor.out(true, "- OrientDBLoader: created property '%s.%s' of type: %s", idxClass, fieldNameParts[0],
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

        index = documentDatabase.getMetadata().getIndexManager().getIndex(idxName);
        if (index != null)
          // ALREADY EXISTS
          continue;

        index = cls.createIndex(idxName, idxType, fields);
        processor.out(true, "- OrientDocumentLoader: created index '%s' type '%s' against Class '%s', fields %s", idxName, idxType,
            idxClass, idxFields);
      }
    }

    documentDatabase.declareIntent(new OIntentMassiveInsert());
  }

  @Override
  public void end() {
    if (dbType == DB_TYPE.DOCUMENT)
      documentDatabase.commit();
    else
      graphDatabase.commit();
  }

  @Override
  public String getName() {
    return "orientdb";
  }

  public String getUnit() {
    return dbType == DB_TYPE.DOCUMENT ? "documents" : "vertices";
  }

  public OrientBaseGraph getGraphDatabase() {
    return graphDatabase;
  }

  public ODatabaseDocumentTx getDocumentDatabase() {
    return documentDatabase;
  }

  protected OClass getOrCreateClass(final String iClassName) {
    OClass cls;

    if (dbType == DB_TYPE.DOCUMENT)
      // DOCUMENT
      if (documentDatabase.getMetadata().getSchema().existsClass(iClassName))
        cls = documentDatabase.getMetadata().getSchema().getClass(iClassName);
      else {
        cls = documentDatabase.getMetadata().getSchema().createClass(iClassName);
        processor.out(true, "- OrientDBLoader: created class '%s'", iClassName);
      }
    else {
      // GRAPH
      cls = graphDatabase.getVertexType(iClassName);
      if (cls == null) {
        cls = graphDatabase.createVertexType(iClassName);
        processor.out(true, "- OrientDBLoader: created vertex class '%s'", iClassName);
      }
    }
    return cls;
  }

}
