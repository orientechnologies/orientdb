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
package com.orientechnologies.orient.core.entity;

import com.orientechnologies.orient.core.config.OStorageClusterHoleConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataHoleConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;

public class OClassDictionary {
	private static final OClassDictionary	instance	= new OClassDictionary();

	public Class<?> getClassByCode(final char iType) {
		switch (iType) {
		case '0':
			return ODocument.class;
			// case '1':
			// return ORecordColumn.class;
		case '2':
			return ORecordFlat.class;
		case '3':
			return ORecordBytes.class;

		case '4':
			return OClass.class;
		case '5':
			return OProperty.class;
		case '6':
			return OUser.class;

		case '7':
			return OStorageConfiguration.class;
		case '8':
			return OStoragePhysicalClusterConfiguration.class;
		case '9':
			return OStorageDataConfiguration.class;
		case 'a':
			return OStorageClusterHoleConfiguration.class;
		case 'b':
			return OStorageDataHoleConfiguration.class;
		case 'c':
			return OStorageSegmentConfiguration.class;
		case 'd':
			return OStorageFileConfiguration.class;
		}

		throw new OConfigurationException("Unsupported record type: " + iType);
	}

	public Character getCodeByClass(final Class<?> iClass) {
		if (iClass.equals(ODocument.class))
			return '0';
		// if (iClass.equals(ORecordColumn.class))
		// return '1';
		if (iClass.equals(ORecordFlat.class))
			return '2';
		if (iClass.equals(ORecordBytes.class))
			return '3';

		if (iClass.equals(OClass.class))
			return '4';
		if (iClass.equals(OProperty.class))
			return '5';
		if (iClass.equals(OUser.class))
			return '6';

		if (iClass.equals(OStorageConfiguration.class))
			return '7';
		if (iClass.equals(OStoragePhysicalClusterConfiguration.class))
			return '8';
		if (iClass.equals(OStorageDataConfiguration.class))
			return '9';
		if (iClass.equals(OStorageClusterHoleConfiguration.class))
			return 'a';
		if (iClass.equals(OStorageDataHoleConfiguration.class))
			return 'b';
		if (iClass.equals(OStorageSegmentConfiguration.class))
			return 'c';
		if (iClass.equals(OStorageFileConfiguration.class))
			return 'd';

		return null;
	}

	public static OClassDictionary instance() {
		return instance;
	}
}
