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
package com.orientechnologies.orient.core.config;

public class OStorageTxConfiguration extends OStorageFileConfiguration {
	private static final String	DEF_EXTENSION				= ".otd";
	private static final String	DEF_MAX_SIZE				= "512mb";
	private static final String	DEF_INCREMENT_SIZE	= "50%";

	private boolean							synchRecord					= false;
	private boolean							synchTx							= true;

	public OStorageTxConfiguration() {
		maxSize = DEF_MAX_SIZE;
	}

	public OStorageTxConfiguration(final String iPath, final String iType, final String iMaxSize, final String iSynchRecord,
			final String iSynchTx) {
		super(null, iPath + DEF_EXTENSION, iType, iMaxSize != null ? iMaxSize : DEF_MAX_SIZE, DEF_INCREMENT_SIZE);

		synchRecord = Boolean.parseBoolean(iSynchRecord);
		synchTx = Boolean.parseBoolean(iSynchTx);
	}

	public boolean isSynchRecord() {
		return synchRecord;
	}

	public boolean isSynchTx() {
		return synchTx;
	}

	public void setSynchRecord(boolean synchRecord) {
		this.synchRecord = synchRecord;
	}

	public void setSynchTx(boolean synchTx) {
		this.synchTx = synchTx;
	}
}
