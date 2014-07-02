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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.util.List;

/**
 * ETL Loader.
 */
public class OOrientDocumentLoader extends OAbstractETLComponent implements OLoader {
  protected long            progress = 0;
  protected String          clusterName;
  protected String          className;
  protected List<ODocument> indexes;
  protected OClass          schemaClass;

  public OOrientDocumentLoader() {
  }

  public void load(final Object input, OCommandContext context) {
    if (input == null)
      return;

    if (className != null && input instanceof ODocument)
      ((ODocument) input).setClassName(className);

    if (clusterName != null)
      ((ORecord) input).save(clusterName);
    else
      ((ORecord) input).save();

    progress++;
  }

  public long getProgress() {
    return progress;
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[{class:{optional:true,description:'Record class name'}},"
        + "{cluster:{optional:true,description:'Cluster name where to store the new record'}},"
        + "{indexes:{optional:true,description:'Index used. It assure the index exist or in case create them'}}],"
        + "input:['ODocument']}");
  }

  @Override
  public void configure(final ODocument iConfiguration) {
    clusterName = iConfiguration.field("cluster");
    className = iConfiguration.field("class");
    indexes = iConfiguration.field("indexes");
  }

  @Override
  public void init(final OETLProcessor iProcessor, final ODatabaseDocumentTx iDatabase) {
    super.init(iProcessor, iDatabase);

    if (className != null) {
      schemaClass = iDatabase.getMetadata().getSchema().getOrCreateClass(className);
      processor.out(true, "%s: found %d records in class '%s'", getName(), schemaClass.count(), className);
    }

    if (indexes != null) {
      for (ODocument idx : indexes) {
        OIndex index;

        String idxName = idx.field("name");
        if (idxName != null) {
          index = iDatabase.getMetadata().getIndexManager().getIndex(idxName);
          if (index != null)
            // ALREADY EXISTS
            continue;
        }

        final String idxClass = idx.field("class");
        if (idxClass == null)
          throw new OConfigurationException("Index 'class' missed in OrientDocument loader");

        final OClass cls;
        if (iDatabase.getMetadata().getSchema().existsClass(idxClass))
          cls = iDatabase.getMetadata().getSchema().getClass(idxClass);
        else {
          cls = iDatabase.getMetadata().getSchema().createClass(idxClass);
          processor.out(true, "- OrientDocumentLoader: created class '%s'", idxClass);
        }

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
            processor.out(true, "- OrientDocumentLoader: created property '%s.%s' of type: %s", idxClass, fieldNameParts[0],
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

        index = iDatabase.getMetadata().getIndexManager().getIndex(idxName);
        if (index != null)
          // ALREADY EXISTS
          continue;

        index = cls.createIndex(idxName, idxType, fields);
        processor.out(true, "- OrientDocumentLoader: created index '%s' type '%s' against Class '%s', fields %s", idxName, idxType,
            idxClass, idxFields);
      }
    }

    iDatabase.declareIntent(new OIntentMassiveInsert());
  }

  @Override
  public String getName() {
    return "orientdb_doc";
  }

  public String getUnit() {
    return "records";
  }

}
