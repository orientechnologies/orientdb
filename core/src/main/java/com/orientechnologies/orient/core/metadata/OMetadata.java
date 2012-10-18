/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryProxy;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.core.metadata.security.OSecurityProxy;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.profiler.OJVMProfiler;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

public class OMetadata {
	public static final String					CLUSTER_INTERNAL_NAME			= "internal";
	public static final String					CLUSTER_INDEX_NAME				= "index";
	public static final String					CLUSTER_MANUAL_INDEX_NAME	= "manindex";

	protected int												schemaClusterId;

	protected OSchemaProxy							schema;
	protected OSecurity									security;
	protected OIndexManagerProxy				indexManager;
	protected OFunctionLibraryProxy			functionLibrary;
	protected static final OJVMProfiler	PROFILER									= Orient.instance().getProfiler();

	public OMetadata() {
	}

	public void load() {
		final long timer = PROFILER.startChrono();

		try {
			init(true);

			if (schemaClusterId == -1 || getDatabase().countClusterElements(CLUSTER_INTERNAL_NAME) == 0)
				return;
		} finally {
			PROFILER.stopChrono(PROFILER.getDatabaseMetric(getDatabase().getName(), "metadata.load"), "Loading of database metadata",
					timer);
		}
	}

	public void create() throws IOException {
		init(false);

		security.create();
		schema.create();
		indexManager.create();
		functionLibrary.create();
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
		final ODatabaseRecord database = getDatabase();
		schemaClusterId = database.getClusterIdByName(CLUSTER_INTERNAL_NAME);

		schema = new OSchemaProxy(database.getStorage().getResource(OSchema.class.getSimpleName(), new Callable<OSchemaShared>() {
			public OSchemaShared call() {
				final OSchemaShared instance = new OSchemaShared(schemaClusterId);
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
							instance.load();

						// rebuild indexes if index cluster wasn't closed properly
						if (iLoad && OGlobalConfiguration.INDEX_AUTO_REBUILD_AFTER_NOTSOFTCLOSE.getValueAsBoolean()
								&& (database.getStorage() instanceof OStorageLocal)
								&& !((OStorageLocal) database.getStorage()).isClusterSoftlyClosed(OMetadata.CLUSTER_INDEX_NAME))
							for (OIndex<?> idx : instance.getIndexes())
								if (idx.isAutomatic()) {
									try {
										OLogManager.instance().info(idx, "Rebuilding index " + idx.getName() + "..");
										idx.rebuild();
									} catch (Exception e) {
										OLogManager.instance().info(idx, "Continue with remaining indexes...");
									}
								}

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

		functionLibrary = new OFunctionLibraryProxy(database.getStorage().getResource(OFunctionLibrary.class.getSimpleName(),
				new Callable<OFunctionLibrary>() {
					public OFunctionLibrary call() {
						final OFunctionLibraryImpl instance = new OFunctionLibraryImpl();
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
		functionLibrary.load();
	}

	/**
	 * Closes internal objects
	 */
	public void close() {
		if (indexManager != null)
			indexManager.flush();
		if (schema != null)
			schema.close();
		if (security != null)
			security.close();
		if (functionLibrary != null)
			functionLibrary.close();
	}

	protected ODatabaseRecord getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	public OFunctionLibrary getFunctionLibrary() {
		return functionLibrary;
	}
}
