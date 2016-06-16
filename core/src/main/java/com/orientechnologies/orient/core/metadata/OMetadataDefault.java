/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.metadata;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCache;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryProxy;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityProxy;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryProxy;
import com.orientechnologies.orient.core.schedule.OScheduler;
import com.orientechnologies.orient.core.schedule.OSchedulerProxy;

import java.io.IOException;
import java.util.concurrent.Callable;

public class OMetadataDefault implements OMetadataInternal {
  public static final String CLUSTER_INTERNAL_NAME     = "internal";
  public static final String CLUSTER_INDEX_NAME        = "index";
  public static final String CLUSTER_MANUAL_INDEX_NAME = "manindex";

  protected int schemaClusterId;

  protected OSchemaProxy          schema;
  protected OSecurity             security;
  protected OIndexManagerProxy    indexManager;
  protected OFunctionLibraryProxy functionLibrary;
  protected OSchedulerProxy       scheduler;
  protected OSequenceLibraryProxy sequenceLibrary;

  protected OCommandCache          commandCache;
  protected static final OProfiler PROFILER = Orient.instance().getProfiler();

  private OImmutableSchema          immutableSchema = null;
  private int                       immutableCount  = 0;
  private ODatabaseDocumentInternal database;

  public OMetadataDefault() {
  }

  public OMetadataDefault(ODatabaseDocumentInternal databaseDocument) {
    this.database = databaseDocument;
  }

  public void load() {
    final long timer = PROFILER.startChrono();

    try {
      OSharedContext shared = init();
      shared.load(database);
    } finally {
      PROFILER.stopChrono(PROFILER.getDatabaseMetric(getDatabase().getName(), "metadata.load"), "Loading of database metadata",
          timer, "db.*.metadata.load");
    }
  }

  public void create() throws IOException {
    OSharedContext shared = init();
    shared.create();
  }

  public OSchemaProxy getSchema() {
    return schema;
  }

  @Override
  public OCommandCache getCommandCache() {
    return commandCache;
  }

  @Override
  public void makeThreadLocalSchemaSnapshot() {
    if (this.immutableCount == 0) {
      if (schema != null)
        this.immutableSchema = schema.makeSnapshot();
    }
    this.immutableCount++;
  }

  @Override
  public void clearThreadLocalSchemaSnapshot() {
    this.immutableCount--;
    if (this.immutableCount == 0) {
      this.immutableSchema = null;
    }
  }

  @Override
  public OImmutableSchema getImmutableSchemaSnapshot() {
    if (immutableSchema == null) {
      if (schema == null)
        return null;
      return schema.makeSnapshot();
    }
    return immutableSchema;
  }

  public OSecurity getSecurity() {
    return security;
  }

  public OIndexManagerProxy getIndexManager() {
    return indexManager;
  }

  public int getSchemaClusterId() {
    return schemaClusterId;
  }

  private OSharedContext init() {
    schemaClusterId = database.getClusterIdByName(CLUSTER_INTERNAL_NAME);

    OSharedContext shared = database.getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContext(database);
        return shared;
      }
    });

    schema = new OSchemaProxy(shared.getSchema(), database);
    indexManager = new OIndexManagerProxy(shared.getIndexManager(), database);
    security = new OSecurityProxy(shared.getSecurity(), database);
    commandCache = shared.getCommandCache();

    final Class<? extends OSecurity> securityClass = (Class<? extends OSecurity>) database
        .getProperty(ODatabase.OPTIONS.SECURITY.toString());
    if (securityClass != null)
      // INSTALL CUSTOM WRAPPED SECURITY
      try {
        final OSecurity wrapped = security;
        security = securityClass.getDeclaredConstructor(OSecurity.class, ODatabaseDocumentInternal.class).newInstance(wrapped,
            database);
      } catch (Exception e) {
        throw OException
            .wrapException(new OSecurityException("Cannot install custom security implementation (" + securityClass + ")"), e);
      }

    functionLibrary = new OFunctionLibraryProxy(shared.getFunctionLibrary(), database);
    sequenceLibrary = new OSequenceLibraryProxy(shared.getSequenceLibrary(), database);
    scheduler = new OSchedulerProxy(shared.getScheduler(), database);
    return shared;
  }

  /**
   * Reloads the internal objects.
   */
  public void reload() {
    //RELOAD ALL THE SHARED CONTEXT
    database.getSharedContext().reload();
    //ADD HERE THE RELOAD OF A PROXY OBJECT IF NEEDED
  }

  /**
   * Closes internal objects
   */
  public void close() {
    //DO NOTHING BECAUSE THE PROXY OBJECT HAVE NO DIRECT STATE
    //ADD HERE THE CLOSE OF A PROXY OBJECT IF NEEDED
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return database;
  }

  public OFunctionLibrary getFunctionLibrary() {
    return functionLibrary;
  }

  @Override
  public OSequenceLibrary getSequenceLibrary() {
    return sequenceLibrary;
  }

  public OScheduler getScheduler() {
    return scheduler;
  }
}
