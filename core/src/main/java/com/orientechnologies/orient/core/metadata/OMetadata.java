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

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.OSecurity;

public class OMetadata {
	public static final String		CLUSTER_METADATA_NAME	= "metadata";

	protected ODatabaseRecord<?>	database;
	protected int									schemaClusterId;

	protected OSchema							schema;
	protected OSecurity						security;

	public OMetadata(ODatabaseRecord<?> iDatabase) {
		this.database = iDatabase;
	}

	public void load() {
		final long timer = OProfiler.getInstance().startChrono();

		init();

		try {
			if (schemaClusterId == -1 || database.countClusterElements(CLUSTER_METADATA_NAME) == 0)
				return;

			loadSchema();
			loadSecurity();
		} finally {
			OProfiler.getInstance().stopChrono("OMetadata.load", timer);
		}
	}

	public void create() throws IOException {
		final long timer = OProfiler.getInstance().startChrono();

		init();

		try {
			// CREATE RECORD FOR SCHEMA
			schema.save(CLUSTER_METADATA_NAME);
			database.getStorage().getConfiguration().schemaRecordId = schema.getIdentity().toString();

			// CREATE RECORD FOR SECURITY
			security.save(CLUSTER_METADATA_NAME);
			database.getStorage().getConfiguration().securityRecordId = security.getIdentity().toString();

			database.getStorage().getConfiguration().update();
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

	public int getSchemaClusterId() {
		return schemaClusterId;
	}

	public void loadSchema() {
		schema.load();
	}

	public void loadSecurity() {
		security.load();
	}

	private void init() {
		schemaClusterId = database.getClusterIdByName(CLUSTER_METADATA_NAME);

		schema = new OSchema(database, schemaClusterId);
		security = new OSecurity(database, schemaClusterId);
	}
}
