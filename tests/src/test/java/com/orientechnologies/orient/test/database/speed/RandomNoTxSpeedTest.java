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
import java.util.Random;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.record.ODatabaseColumn;
import com.orientechnologies.orient.core.query.nativ.ONativeSynchQuery;
import com.orientechnologies.orient.core.query.nativ.OQueryContextNativePositional;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;

public class RandomNoTxSpeedTest extends SpeedTestMonoThread {
	private static final String	CLUSTER_NAME	= "Animal";

	private ODatabaseColumn	database;
	private Random							random;
	private ORecordColumn					record;

	public RandomNoTxSpeedTest() {
		super(1000000);
		random = new Random(System.currentTimeMillis());
	}

	public void init() throws IOException {
		if (!database.getStorage().getClusterNames().contains(CLUSTER_NAME))
			database.getStorage().addCluster(CLUSTER_NAME);
	}

	public void cycle() throws UnsupportedEncodingException {
		long clusterCount = database.countClusterElements(CLUSTER_NAME);

		switch (random.nextInt(3)) {
		case 0:
			// CREATE RECORD
			record.value(clusterCount + "|Gipsy|Cat|European|Italy|" + (clusterCount + 300) + ".00").save(CLUSTER_NAME);
			break;
		case 1:
			// DELETE RECORD
			if (clusterCount > 0)
				// database.deleteRecord( CLUSTER_NAME, random.nextInt((int) (clusterCount - 1)));
				break;
		case 2:
			// UPDATE RECORD
			if (clusterCount > 0)
				record.value(clusterCount + "|Gipsy|Cat|European|Italy|" + (clusterCount + 3000) + ".00").save();
			break;
		case 3:
			// QUERY RECORDS
			final int counter = random.nextInt((int) (clusterCount - 1));

			database.query(
					new ONativeSynchQuery<ORecordColumn, OQueryContextNativePositional<ORecordColumn>>(CLUSTER_NAME,
							new OQueryContextNativePositional<ORecordColumn>()) {

						@Override
						public boolean filter(OQueryContextNativePositional<ORecordColumn> iRecord) {
							return iRecord.column(0).toInt().eq(counter).go();
						}

					}).execute();
			break;
		}

		record.value(data.getCyclesDone() + "|Gipsy|Cat|European|Italy|" + (data.getCyclesDone() + 300) + ".00").save(CLUSTER_NAME);
	}
}
