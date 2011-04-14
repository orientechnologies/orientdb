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
package com.orientechnologies.orient.test.java.lang;

import java.util.TreeMap;

import com.orientechnologies.common.collection.OMVRBTreeMemory;

public class TreeMapsSpeedTest {
	private static final long		MAX		= 1000000;
	private static final Object	DUMMY	= new Object();

	public static final void main(String[] args) {

		long timer = System.currentTimeMillis();
		for (int i = 0; i < MAX; ++i) {
		}
		final long fixed = System.currentTimeMillis() - timer;

		OMVRBTreeMemory<Integer, Object> orientTreeMap = new OMVRBTreeMemory<Integer, Object>(232000, 0.75f);

		for (int i = 0; i < MAX; ++i) {
			orientTreeMap.put(i, DUMMY);
		}

		System.out.println("OrientDB Tree Map insertion: " + (System.currentTimeMillis() - timer - fixed));
		timer = System.currentTimeMillis();

		for (int i = 0; i < MAX; ++i) {
			if (orientTreeMap.get(i) != DUMMY) {
				System.out.println("Error in map content");
			}
		}

		System.out.println("OrientDB Tree Map read: " + (System.currentTimeMillis() - timer - fixed));

		orientTreeMap.clear();
		orientTreeMap = null;

		timer = System.currentTimeMillis();

		TreeMap<Integer, Object> javaTreeMap = new TreeMap<Integer, Object>();

		for (int i = 0; i < MAX; ++i) {
			javaTreeMap.put(i, DUMMY);
		}

		System.out.println("Java Tree Map insertion: " + (System.currentTimeMillis() - timer - fixed));
		timer = System.currentTimeMillis();

		for (int i = 0; i < MAX; ++i) {
			if (javaTreeMap.get(i) != DUMMY) {
				System.out.println("Error in map content");
			}
		}

		System.out.println("Java Tree Map read: " + (System.currentTimeMillis() - timer - fixed));
		timer = System.currentTimeMillis();

		javaTreeMap.clear();
		javaTreeMap = null;
	}
}
