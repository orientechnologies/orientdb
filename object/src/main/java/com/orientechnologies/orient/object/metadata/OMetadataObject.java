/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.object.metadata;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.core.schedule.OScheduler;
import com.orientechnologies.orient.object.metadata.schema.OSchemaProxyObject;
import java.io.IOException;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class OMetadataObject implements OMetadataInternal {

  protected OMetadataInternal underlying;
  protected OSchemaProxyObject schema;
  private ODatabaseDocumentInternal database;

  public OMetadataObject(OMetadataInternal iUnderlying, ODatabaseDocumentInternal database) {
    underlying = iUnderlying;
    this.database = database;
  }

  public OMetadataObject(OMetadataInternal iUnderlying, OSchemaProxyObject iSchema) {
    underlying = iUnderlying;
    schema = iSchema;
  }

  @Override
  public void makeThreadLocalSchemaSnapshot() {
    underlying.makeThreadLocalSchemaSnapshot();
  }

  @Override
  public void clearThreadLocalSchemaSnapshot() {
    underlying.clearThreadLocalSchemaSnapshot();
  }

  @Override
  public OImmutableSchema getImmutableSchemaSnapshot() {
    return underlying.getImmutableSchemaSnapshot();
  }

  @Override
  @Deprecated
  public void load() {
    underlying.load();
  }

  @Override
  @Deprecated
  public void create() throws IOException {
    underlying.create();
  }

  @Override
  public OSchemaProxyObject getSchema() {
    if (schema == null) schema = new OSchemaProxyObject(underlying.getSchema());
    return schema;
  }

  @Override
  public OSecurity getSecurity() {
    return underlying.getSecurity();
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public OIndexManager getIndexManager() {
    return new OIndexManagerProxy(underlying.getIndexManagerInternal(), database);
  }

  @Override
  public OIndexManagerAbstract getIndexManagerInternal() {
    return underlying.getIndexManagerInternal();
  }

  @Override
  public int getSchemaClusterId() {
    return underlying.getSchemaClusterId();
  }

  @Override
  public void reload() {
    underlying.reload();
  }

  @Override
  @Deprecated
  public void close() {
    underlying.close();
  }

  @Override
  public OFunctionLibrary getFunctionLibrary() {
    return underlying.getFunctionLibrary();
  }

  @Override
  public OSequenceLibrary getSequenceLibrary() {
    return underlying.getSequenceLibrary();
  }

  @Override
  public OScheduler getScheduler() {
    return underlying.getScheduler();
  }

  public OMetadata getUnderlying() {
    return underlying;
  }
}
