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
package com.orientechnologies.orient.test.java.collection;

import com.orientechnologies.common.test.SpeedTestGroup;

public class CollectionSpeedTest extends SpeedTestGroup {
	protected static final int		TEST_CYCLES			= 10000000;
	protected static final int		COLLECTION_SIZE	= 10;
	protected static final String	SEARCHED_VALUE	= String.valueOf(COLLECTION_SIZE - 1);

	public void testOnce() {
		addTest(new ArrayListSpeedTest()).data().setCycles(TEST_CYCLES).config(COLLECTION_SIZE, SEARCHED_VALUE);

		addTest(new HashMapSpeedTest()).data().setCycles(TEST_CYCLES).config(COLLECTION_SIZE, SEARCHED_VALUE);

		addTest(new ArraySpeedTest()).data().setCycles(TEST_CYCLES).config(COLLECTION_SIZE, SEARCHED_VALUE);

		go();
	}
}
