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

import java.io.IOException;
import java.util.concurrent.Callable;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryProxy;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityProxy;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.schedule.OSchedulerListener;
import com.orientechnologies.orient.core.schedule.OSchedulerListenerImpl;
import com.orientechnologies.orient.core.schedule.OSchedulerListenerProxy;
import com.orientechnologies.orient.core.storage.OStorageProxy;

public class OMetadataDefault implements OMetadataInternal {
  public static final String        CLUSTER_INTERNAL_NAME     = "internal";
  public static final String        CLUSTER_INDEX_NAME        = "index";
  public static final String        CLUSTER_MANUAL_INDEX_NAME = "manindex";

  protected int                     schemaClusterId;

  protected OSchemaProxy            schema;
  protected OSecurity               security;
  protected OIndexManagerProxy      indexManager;
  protected OFunctionLibraryProxy   functionLibrary;
  protected OSchedulerListenerProxy scheduler;

  protected static final OProfiler  PROFILER                  = Orient.instance().getProfiler();

  private OImmutableSchema          immutableSchema           = null;
  private int                       immutableCount            = 0;

  public OMetadataDefault() {
  }

  public void load() {
    final long timer = PROFILER.startChrono();

    try {
      init(true);

      if (schemaClusterId == -1 || getDatabase().countClusterElements(CLUSTER_INTERNAL_NAME) == 0)
        return;

    } finally {
      PROFILER.stopChrono(PROFILER.getDatabaseMetric(getDatabase().getName(), "metadata.load"), "Loading of database metadata",
          timer, "db.*.metadata.load");
    }
  }

  public void create() throws IOException {
    init(false);

    schema.create();
    indexManager.create();
    security.create();
    functionLibrary.create();
    security.createClassTrigger();
    scheduler.create();
  }

  public OSchemaProxy getSchema() {
    return schema;
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

  private void init(final boolean iLoad) {
    final ODatabaseDocumentInternal database = getDatabase();
    schemaClusterId = database.getClusterIdByName(CLUSTER_INTERNAL_NAME);

    schema = new OSchemaProxy(database.getStorage().getResource(OSchema.class.getSimpleName(), new Callable<OSchemaShared>() {
      public OSchemaShared call() {
        ODatabaseDocumentInternal database = getDatabase();
        final OSchemaShared instance = new OSchemaShared(database.getStorageVersions().classesAreDetectedByClusterId());
        if (iLoad)
          instance.load();
        return instance;
      }
    }), database);

    indexManager = new OIndexManagerProxy(database.getStorage().getResource(OIndexManager.class.getSimpleName(),
        new Callable<OIndexManager>() {
          public OIndexManager call() {
            OIndexManager instance;
            if (database.getStorage() instanceof OStorageProxy)
              instance = new OIndexManagerRemote(database);
            else
              instance = new OIndexManagerShared(database);

            if (iLoad)
              try {
                instance.load();
              } catch (Exception e) {
                OLogManager.instance().error(this, "[OMetadata] Error on loading index manager, reset index configuration", e);
                instance.create();
              }

            return instance;
          }
        }), database);

    security = new OSecurityProxy(database.getStorage().getResource(OSecurity.class.getSimpleName(),
        new Callable<OSecurityShared>() {
          public OSecurityShared call() {
            final OSecurityShared instance = new OSecurityShared();
            if (iLoad) {
              security = instance;
              instance.load();
            }
            return instance;
          }
        }), database);

    final Class<? extends OSecurity> securityClass = (Class<? extends OSecurity>) database.getProperty(ODatabase.OPTIONS.SECURITY
        .toString());
    if (securityClass != null)
      // INSTALL CUSTOM WRAPPED SECURITY
      try {
        final OSecurity wrapped = security;
        security = securityClass.getDeclaredConstructor(OSecurity.class, ODatabaseDocumentInternal.class).newInstance(wrapped,
            database);
      } catch (Exception e) {
        throw new OSecurityException("Cannot install custom security implementation (" + securityClass + ")", e);
      }

    functionLibrary = new OFunctionLibraryProxy(database.getStorage().getResource(OFunctionLibrary.class.getSimpleName(),
        new Callable<OFunctionLibrary>() {
          public OFunctionLibrary call() {
            final OFunctionLibraryImpl instance = new OFunctionLibraryImpl();
            if (iLoad)
              instance.load();
            return instance;
          }
        }), database);
    scheduler = new OSchedulerListenerProxy(database.getStorage().getResource(OSchedulerListener.class.getSimpleName(),
        new Callable<OSchedulerListener>() {
          public OSchedulerListener call() {
            final OSchedulerListenerImpl instance = new OSchedulerListenerImpl();
            if (iLoad)
              instance.load();
            return instance;
          }
        }), database);
  }

  /**
   * Reloads the internal objects.
   */
  public void reload() {
    if (schema != null)
      schema.reload();
    if (indexManager != null)
      indexManager.reload();
    if (security != null)
      security.load();
    if (functionLibrary != null)
      functionLibrary.load();
  }

  /**
   * Closes internal objects
   */
  public void close() {
    if (schema != null)
      schema.close();
    if (security != null)
      security.close(false);
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public OFunctionLibrary getFunctionLibrary() {
    return functionLibrary;
  }

  public OSchedulerListener getSchedulerListener() {
    return scheduler;
  }
}
