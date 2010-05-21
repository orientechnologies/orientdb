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

public class OStorageFileConfiguration {

	public transient OStorageSegmentConfiguration	parent;

	public String																	path;
	public String																	type					= "mmap";
	public String																	maxSize				= null;
	public String																	incrementSize	= "50%";

	public OStorageFileConfiguration() {
	}

	public OStorageFileConfiguration(final OStorageSegmentConfiguration iParent, final String iPath, final String iType,
			final String iMaxSize, String iIncrementSize) {
		parent = iParent;
		path = iPath;
		type = iType;
		maxSize = iMaxSize;
		incrementSize = iIncrementSize;
	}

	@Override
	public String toString() {
		return path;
	}
}
