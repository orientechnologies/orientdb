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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.cache.OCacheRecord;
import com.orientechnologies.orient.core.config.OConfigurationConstants;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public abstract class OStorageAbstract extends OSharedResourceAdaptive implements OStorage {
	protected OStorageConfiguration	configuration;
	protected String								name;
	protected String								url;
	protected String								mode;
	protected OCacheRecord					cache	= new OCacheRecord(20000);

	protected boolean								open	= false;

	public OStorageAbstract(final String iName, final String iFilePath, final String iMode) {
		if (OStringSerializerHelper.contains(iName, '/'))
			name = iName.substring(iName.lastIndexOf("/") + 1);
		else
			name = iName;

		if (OStringSerializerHelper.contains(iName, ','))
			throw new IllegalArgumentException("Invalid character in storage name: " + name);

		url = iFilePath;
		mode = iMode;
	}

	public int updateRecord(final int iRequesterId, final ORID iRecordId, final byte[] iContent, final int iVersion,
			final byte iRecordType) {
		return updateRecord(iRequesterId, iRecordId.getClusterId(), iRecordId.getClusterPosition(), iContent, iVersion, iRecordType);
	}

	public boolean deleteRecord(final int iRequesterId, final ORID iRecordId, final int iVersion) {
		return deleteRecord(iRequesterId, iRecordId.getClusterId(), iRecordId.getClusterPosition(), iVersion);
	}

	public OStorageConfiguration getConfiguration() {
		return configuration;
	}

	public boolean isClosed() {
		return !open;
	}

	public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
		return ppos != null && ppos.version != -1;
	}

	public String getName() {
		return name;
	}

	public String getURL() {
		return url;
	}

	@Override
	public int removeUser() {
		int u = super.removeUser();

		boolean keepOpen = Boolean.parseBoolean(System.getProperty(OConfigurationConstants.KEEP_STORAGE_OPEN));
		if (u == 0 && !keepOpen)
			close();

		return u;
	}

	public void close(boolean iForce) {
		close();
	}

	public boolean removeCluster(final String iClusterName) {
		return removeCluster(getClusterIdByName(iClusterName));
	}

	public OCacheRecord getCache() {
		return cache;
	}
}
