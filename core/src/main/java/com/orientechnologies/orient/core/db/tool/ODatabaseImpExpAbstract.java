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
package com.orientechnologies.orient.core.db.tool;

import java.util.LinkedHashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * Abstract class for import/export of database and data in general.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class ODatabaseImpExpAbstract {
	protected ODatabaseRecord					database;
	protected String									fileName;

	protected boolean									includeInfo			= true;
	protected Set<String>							includeClusters;
	protected Set<String>							excludeClusters;
	protected Set<String>							includeClasses;
	protected Set<String>							excludeClasses;
	protected boolean									includeSchema		= true;
	protected boolean									includeSecurity	= false;
	protected OCommandOutputListener	listener;

	public ODatabaseImpExpAbstract(final ODatabaseRecord iDatabase, final String iFileName, final OCommandOutputListener iListener) {
		database = iDatabase;
		fileName = iFileName;
		listener = iListener;
		excludeClusters = new LinkedHashSet<String>();
	}

	public Set<String> getIncludeClusters() {
		return includeClusters;
	}

	public void setIncludeClusters(Set<String> includeClusters) {
		this.includeClusters = includeClusters;
	}

	public Set<String> getExcludeClusters() {
		return excludeClusters;
	}

	public void setExcludeClusters(Set<String> excludeClusters) {
		this.excludeClusters = excludeClusters;
	}

	public Set<String> getIncludeClasses() {
		return includeClasses;
	}

	public void setIncludeClasses(Set<String> includeClasses) {
		this.includeClasses = includeClasses;
	}

	public Set<String> getExcludeClasses() {
		return excludeClasses;
	}

	public void setExcludeClasses(Set<String> excludeClasses) {
		this.excludeClasses = excludeClasses;
	}

	public OCommandOutputListener getListener() {
		return listener;
	}

	public void setListener(OCommandOutputListener listener) {
		this.listener = listener;
	}

	public ODatabaseRecord getDatabase() {
		return database;
	}

	public String getFileName() {
		return fileName;
	}

	public boolean isIncludeSchema() {
		return includeSchema;
	}

	public void setIncludeSchema(boolean includeSchema) {
		this.includeSchema = includeSchema;
	}
}
