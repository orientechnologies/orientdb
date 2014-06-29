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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * ETL Loader.
 */
public class OOrientLoader implements OLoader {
  protected ODatabaseDocumentTx db;
  protected long                progress = 0;
  protected String              clusterName;
  protected String              className;
  protected OClass              schemaClass;

  public OOrientLoader() {
  }

  public void load(final Object input, OCommandContext context) {
    if (input == null)
      return;

    if (!(input instanceof ORecord<?>))
      throw new IllegalArgumentException("Expected record but received object '" + input + "' of class: " + input.getClass());

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
  public void configure(final ODocument iConfiguration) {
    if (iConfiguration.containsField("cluster"))
      clusterName = iConfiguration.field("cluster");
    if (iConfiguration.containsField("class"))
      className = iConfiguration.field("class");
  }

  @Override
  public void prepare(final ODatabaseDocumentTx iDatabase) {
    this.db = iDatabase;
    if (className != null) {
      schemaClass = iDatabase.getMetadata().getSchema().getOrCreateClass(className);
      OLogManager.instance().info(this, "Found %d records in class '%s'", schemaClass.count(), className);
    }
  }

  @Override
  public String getName() {
    return "orient";
  }

  public String getUnit() {
    return "records";
  }

}
