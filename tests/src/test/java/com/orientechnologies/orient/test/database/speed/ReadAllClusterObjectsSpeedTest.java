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
package com.orientechnologies.orient.test.database.speed;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;

public class ReadAllClusterObjectsSpeedTest extends SpeedTestMonoThread {
	private static final String	CLUSTER_NAME	= "Animal";
	private final static int		RECORDS				= 1;
	private ODatabaseRaw				db;
	private int									objectsRead;

	public ReadAllClusterObjectsSpeedTest() {
		super(RECORDS);
	}

	@Override
	public void init() throws IOException {
		db = new ODatabaseRaw("embedded:database/test");
	}

	@Override
	public void cycle() throws UnsupportedEncodingException {
		ORawBuffer buffer;
		objectsRead = 0;

		int clusterId = db.getClusterIdByName(CLUSTER_NAME);

		final ORecordId rid = new ORecordId(clusterId);
		for (int i = 0; i < db.countClusterElements(CLUSTER_NAME); ++i) {
			rid.clusterPosition = i;
			
			buffer = db.read(rid, null);
			if (buffer != null)
				++objectsRead;
		}
	}

	@Override
	public void deinit() throws IOException {
		System.out.println("Read " + objectsRead + " objects in the cluster " + CLUSTER_NAME);
		db.close();
	}
}
