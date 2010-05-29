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
package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordWrapperAbstract;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.iterator.ORecordIteratorMultiCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings("unchecked")
public class ODatabaseDocumentTx extends ODatabaseRecordWrapperAbstract<ODatabaseRecordTx<ODocument>, ODocument> implements
		ODatabaseDocument {

	public ODatabaseDocumentTx(final String iURL) {
		super(new ODatabaseRecordTx<ODocument>(iURL, ODocument.class));
	}

	@Override
	public ODocument newInstance() {
		return new ODocument(this);
	}

	public ODocument newInstance(final String iClassName) {
		checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, iClassName);

		return new ODocument(this, iClassName);
	}

	public ORecordIteratorMultiCluster<ODocument> browseClass(final String iClassName) {
		if (getMetadata().getSchema().getClass(iClassName) == null)
			throw new IllegalArgumentException("Class '" + iClassName + "' not found in current database");

		checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, iClassName);

		return new ORecordIteratorMultiCluster<ODocument>(this, underlying, getMetadata().getSchema().getClass(iClassName)
				.getClusterIds());
	}

	@Override
	public ORecordIteratorCluster<ODocument> browseCluster(final String iClusterName) {
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

		return new ORecordIteratorCluster<ODocument>(this, underlying, getClusterIdByName(iClusterName));
	}

	/**
	 * If the record is new and a class was specified, the configured cluster id will be used to store the class.
	 */
	@Override
	public ODatabaseDocumentTx save(final ODocument iContent) {
		iContent.validate();

		try {
			if (!iContent.getIdentity().isValid()) {
				// NEW RECORD
				if (iContent.getClassName() != null)
					checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, iContent.getClassName());

				if (iContent.getSchemaClass() != null) {
					// CLASS FOUND: FORCE THE STORING IN THE CLUSTER CONFIGURED
					String clusterName = getClusterNameById(iContent.getSchemaClass().getDefaultClusterId());

					super.save(iContent, clusterName);
					return this;
				}
			} else {
				// UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
				if (iContent.getClassName() != null)
					checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_UPDATE, iContent.getClassName());
			}

			super.save(iContent);

		} catch (Exception e) {
			OLogManager.instance().exception("Error on saving record #%s of class '%s'", e, ODatabaseException.class,
					iContent.getIdentity(), (iContent.getClassName() != null ? iContent.getClassName() : "?"));
		}
		return this;
	}

	/**
	 * Store the record on the specified cluster only after having checked the cluster is allowed and figures in the configured and
	 * the record is valid following the constraints declared in the schema.
	 * 
	 * @see ORecordSchemaAware#validate()
	 */
	@Override
	public ODatabaseDocumentTx save(final ODocument iContent, String iClusterName) {
		if (!iContent.getIdentity().isValid()) {
			if (iClusterName == null && iContent.getSchemaClass() != null)
				// FIND THE RIGHT CLUSTER AS CONFIGURED IN CLASS
				iClusterName = getClusterNameById(iContent.getSchemaClass().getDefaultClusterId());

			int id = getClusterIdByName(iClusterName);
			if (id == -1)
				throw new IllegalArgumentException("Cluster name " + iClusterName + " is not configured");

			if (iContent.getSchemaClass() == null)
				throw new IllegalArgumentException("Class not configured in the record to save");

			// CHECK IF THE CLUSTER IS PART OF THE CONFIGURED CLUSTERS
			int[] clusterIds = iContent.getSchemaClass().getClusterIds();
			int i = 0;
			for (; i < clusterIds.length; ++i)
				if (clusterIds[i] == id)
					break;

			if (id == clusterIds.length)
				throw new IllegalArgumentException("Cluster name " + iClusterName + " is not configured to store the class "
						+ iContent.getClassName());
		}

		iContent.validate();

		super.save(iContent, iClusterName);
		return this;
	}

	@Override
	public ODatabaseDocumentTx delete(final ODocument iContent) {
		// CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
		if (iContent.getClassName() != null)
			checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_DELETE, iContent.getClassName());

		try {
			underlying.delete(iContent);

		} catch (Exception e) {
			OLogManager.instance().exception("Error on deleting record #%s of class '%s'", e, ODatabaseException.class,
					iContent.getIdentity(), iContent.getClassName());
		}
		return this;
	}

	public long countClass(final String iClassName) {
		OClass cls = getMetadata().getSchema().getClass(iClassName);

		if (cls == null)
			throw new IllegalArgumentException("Class '" + iClassName + "' not found in database");

		return countClusterElements(getMetadata().getSchema().getClass(iClassName).getClusterIds());
	}
}
