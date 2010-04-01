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
package com.orientechnologies.orient.core.db;

import java.util.Collection;

import com.orientechnologies.orient.core.cache.OCacheRecord;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Limits:
 * <ul>
 * <li>Maximum records per cluster/class = <b>9.223.372.036 Billions</b>: 2^63 = 9.223.372.036.854.775.808 records</li>
 * <li>Maximum records per database = <b>302.231.454.903.657 Billions</b>: 2^15 clusters x 2^63 records = (2^78) 32.768 *
 * 9,223.372.036.854.775.808 = 302.231,454.903.657.293.676.544 records</li>
 * <li>Maximum storage per data-segment = <b>9.223.372 Terabytes</b>: 2^63 bytes = 9,223.372.036.854.775.808 Exabytes</li>
 * <li>Maximum storage per database = <b>19.807.040.628.566.084 Terabytes</b>: 2^31 data-segments x 2^63 bytes = (2^94)
 * 2.147.483.648 x 9,223.372.036.854.775.808 Exabytes = 19.807,040.628.566.084.398.385.987.584 Yottabytes</li>
 * </ul>
 * 
 * @author Luca Garulli
 * 
 */
public interface ODatabase {
	public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword);

	public <DB extends ODatabase> DB create(final String iStorageMode);

	public void declareIntent(String iIntentType, Object... iParams);

	public boolean exists();

	public void close();

	public int getId();

	public String getName();

	public OStorage getStorage();

	public OCacheRecord getCache();

	public int getDefaultClusterId();

	public Collection<String> getClusterNames();

	public int getClusterIdByName(String iClusterName);

	public String getClusterNameById(int iClusterId);

	public boolean isClosed();

	public long countClusterElements(int currentClusterId);

	public long countClusterElements(int[] iClusterIds);

	public long countClusterElements(String iClassName);

	public int addLogicalCluster(String iClassName, int iPhyClusterContainerId);
	
	public int addPhysicalCluster(String iClusterName, String iClusterFileName, int iStartSize);

	public int addDataSegment(String iSegmentName, String iSegmentFileName);

	public <DB extends ODatabase> DB checkSecurity(String iResource, int iOperation);
}
