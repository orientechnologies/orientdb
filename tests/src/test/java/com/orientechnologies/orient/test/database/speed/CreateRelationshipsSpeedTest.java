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
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.storage.OStorage;

public class CreateRelationshipsSpeedTest extends SpeedTestMonoThread {
	private ODatabaseFlat	database;
	private ORecordFlat		record;

	public CreateRelationshipsSpeedTest() {
		super(1000000);
	}

	@Override
	public void init() throws IOException {
		if (!database.getClusterNames().contains("Animal"))
			database.addCluster("Animal", OStorage.CLUSTER_TYPE.PHYSICAL);

		if (!database.getClusterNames().contains("Vaccinate"))
			database.getStorage().addCluster("Vaccinate", OStorage.CLUSTER_TYPE.PHYSICAL);
	}

	@Override
	public void cycle() throws UnsupportedEncodingException {
		record.value(data.getCyclesDone() + "|" + System.currentTimeMillis() + "|AAA").save("Vaccinate");
		record.value(
				data.getCyclesDone() + "|Gipsy|Cat|European|Italy|" + (data.getCyclesDone() + 300) + ".00|#Vaccinate:"
						+ data.getCyclesDone()).save("Animal");
	}
}
