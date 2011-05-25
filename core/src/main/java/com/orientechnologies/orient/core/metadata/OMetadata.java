/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.metadata;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.core.metadata.security.OSecurityProxy;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.storage.OStorage;

public class OMetadata {
	protected ODatabaseRecord	database;
	protected int							schemaClusterId;

	protected OSchemaProxy		schema;
	protected OSecurity				security;
	protected OIndexManager		indexManager;

	public OMetadata(final ODatabaseRecord iDatabase) {
		this.database = iDatabase;
	}

	public void load() {
		final long timer = OProfiler.getInstance().startChrono();

		init();

		try {
			if (schemaClusterId == -1 || database.countClusterElements(OStorage.CLUSTER_INTERNAL_NAME) == 0)
				return;

			indexManager.load();
			security.load();
			schema.load();
		} finally {
			OProfiler.getInstance().stopChrono("OMetadata.load", timer);
		}
	}

	public void create() throws IOException {
		final long timer = OProfiler.getInstance().startChrono();

		init();

		try {
			// CREATE RECORD FOR SCHEMA
			schema.create();
			security.create();
			indexManager.create();
		} finally {

			OProfiler.getInstance().stopChrono("OMetadata.create", timer);
		}
	}

	public OSchema getSchema() {
		return schema;
	}

	public OSecurity getSecurity() {
		return security;
	}

	public OIndexManager getIndexManager() {
		return indexManager;
	}

	public int getSchemaClusterId() {
		return schemaClusterId;
	}

	private void init() {
		schemaClusterId = database.getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME);

		schema = new OSchemaProxy(database.getStorage().getResource(OSchema.class.getSimpleName(), new Callable<OSchemaShared>() {
			public OSchemaShared call() {
				return new OSchemaShared(schemaClusterId);
			}
		}), database);

		if (database.getProperty(ODatabase.OPTIONS.NOSCHEMA.toString()) != null)
			// INSTALL NO SECURITY IMPL
			security = new OSecurityNull();
		else
			security = new OSecurityProxy(database.getStorage().getResource(OSecurity.class.getSimpleName(),
					new Callable<OSecurityShared>() {
						public OSecurityShared call() {
							return new OSecurityShared();
						}
					}), database);

		indexManager = new OIndexManagerImpl(database);
	}
}
