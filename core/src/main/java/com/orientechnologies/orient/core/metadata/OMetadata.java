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
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.core.metadata.security.OSecurityProxy;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

public class OMetadata {
	protected ODatabaseRecord			database;
	protected int									schemaClusterId;

	protected OSchemaProxy				schema;
	protected OSecurity						security;
	protected OIndexManagerProxy	indexManager;

	public OMetadata(final ODatabaseRecord iDatabase) {
		this.database = iDatabase;
	}

	public void load() {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			init(true);

			if (schemaClusterId == -1 || database.countClusterElements(OStorage.CLUSTER_INTERNAL_NAME) == 0)
				return;
		} finally {
			OProfiler.getInstance().stopChrono("OMetadata.load", timer);
		}
	}

	public void create() throws IOException {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			init(false);

			schema.create();
			security.create();
			indexManager.create();
		} finally {
			OProfiler.getInstance().stopChrono("OMetadata.load", timer);
		}
	}

	public OSchema getSchema() {
		return schema;
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
		// ODatabaseRecordThreadLocal.INSTANCE.set(database);

		schemaClusterId = database.getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME);

		indexManager = new OIndexManagerProxy(database.getStorage().getResource(OIndexManager.class.getSimpleName(),
				new Callable<OIndexManager>() {
					public OIndexManager call() {
						OIndexManager instance;
						if (database.getStorage() instanceof OStorageEmbedded)
							instance = new OIndexManagerShared(database);
						else
							instance = new OIndexManagerRemote(database);

						if (iLoad)
							instance.load();

						return instance;
					}
				}), database);

		schema = new OSchemaProxy(database.getStorage().getResource(OSchema.class.getSimpleName(), new Callable<OSchemaShared>() {
			public OSchemaShared call() {
				final OSchemaShared instance = new OSchemaShared(schemaClusterId);
				if (iLoad)
					instance.load();
				return instance;
			}
		}), database);

		final Boolean enableSecurity = (Boolean) database.getProperty(ODatabase.OPTIONS.SECURITY.toString());
		if (enableSecurity != null && !enableSecurity)
			// INSTALL NO SECURITY IMPL
			security = new OSecurityNull();
		else
			security = new OSecurityProxy(database.getStorage().getResource(OSecurity.class.getSimpleName(),
					new Callable<OSecurityShared>() {
						public OSecurityShared call() {
							final OSecurityShared instance = new OSecurityShared();
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
		schema.reload();
		indexManager.load();
		security.load();
	}
	

	/**
	 * Closes internal objects
	 */
	public void close() {
		indexManager.flush();
		schema.close();
		security.close();
	}
}
