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

import java.io.Serializable;

@SuppressWarnings("serial")
public class OStorageSegmentConfiguration implements Serializable {
	public transient OStorageConfiguration	root;

	public int															id;
	public String														name;
	public String														maxSize						= "0";
	public String														fileType					= "mmap";
	public String														fileStartSize			= "500Kb";
	public String														fileMaxSize				= "500Mb";
	public String														fileIncrementSize	= "50%";
	public String														defrag						= "auto";

	public OStorageFileConfiguration[]			infoFiles;
	String																	directory;

	public OStorageSegmentConfiguration() {
		infoFiles = new OStorageFileConfiguration[0];
	}

	public OStorageSegmentConfiguration(final OStorageConfiguration iRoot, final String iSegmentName, final int iId) {
		root = iRoot;
		name = iSegmentName;
		id = iId;
		infoFiles = new OStorageFileConfiguration[0];
	}

	public OStorageSegmentConfiguration(final OStorageConfiguration iRoot, final String iSegmentName, final int iId,
			final String iDirectory) {
		root = iRoot;
		name = iSegmentName;
		id = iId;
		directory = iDirectory;
		infoFiles = new OStorageFileConfiguration[0];
	}

	public void setRoot(OStorageConfiguration iRoot) {
		this.root = iRoot;
		for (OStorageFileConfiguration f : infoFiles)
			f.parent = this;
	}

	public String getDirectory() {
		if (directory != null)
			return directory;

		return root != null ? root.getDirectory() : null;
	}
}
