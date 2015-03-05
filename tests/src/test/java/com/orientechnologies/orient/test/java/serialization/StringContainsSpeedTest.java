/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.test.java.serialization;

public class StringContainsSpeedTest {
	private static final String	TEXT	= "Ciao, questa, e, una, prova. Che ne pensi?";
	private static final int		MAX		= 10000000;

	public static void main(String[] iArgs) {
		System.out.println("Start testing cycle X " + MAX);

		long time = System.currentTimeMillis();
		for (int i = 0; i < MAX; ++i) {
			TEXT.contains(",");
			if (i % (MAX / 10) == 0) {
                            System.out.print(".");
                        }
		}
		System.out.println("\nElapsed: " + (System.currentTimeMillis() - time));
	}
}
