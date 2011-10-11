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
package com.orientechnologies.orient.graph.gremlin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class OCloneHelper {
	/*
	 * Tries to clone any Java object by using 3 techniques: - instanceof (most verbose but faster performance) - reflection (medium
	 * performance) - serialization (applies for any object type but has a performance overhead)
	 */
	public static Object cloneObject(Object objectToClone, Object previousClone) {

		// ***************************************************************************************************************************************
		// 1. Class by class cloning (only clones known types)
		// ***************************************************************************************************************************************
		// Clone any Map (shallow clone should be enough at this level)
		if (objectToClone instanceof Map) {
			Map recycledMap = (Map) previousClone;
			if (recycledMap == null)
				recycledMap = new HashMap();
			else
				recycledMap.clear();
			recycledMap.putAll((Map) objectToClone);
			return recycledMap;

			// Clone any collection (shallow clone should be enough at this level)
		} else if (objectToClone instanceof Collection) {
			Collection recycledCollection = (Collection) previousClone;
			if (recycledCollection == null)
				recycledCollection = new ArrayList();
			else
				recycledCollection.clear();
			recycledCollection.addAll((Collection) objectToClone);
			return recycledCollection;

			// Clone String
		} else if (objectToClone instanceof String) {
			return objectToClone;

			// Clone Date
		} else if (objectToClone instanceof Date) {
			Date clonedDate = (Date) ((Date) objectToClone).clone();
			return clonedDate;

		} else {
			// ***************************************************************************************************************************************
			// 2. Polymorphic clone (by reflection, looks for a clone() method in hierarchy and invoke it)
			// ***************************************************************************************************************************************
			try {
				Object newClone = null;
				for (Class obj = objectToClone.getClass(); !obj.equals(Object.class); obj = obj.getSuperclass()) {
					Method m[] = obj.getDeclaredMethods();
					for (int i = 0; i < m.length; i++) {
						if (m[i].getName().equals("clone")) {
							m[i].setAccessible(true);
							newClone = m[i].invoke(objectToClone);
							System.out.println(objectToClone.getClass()
									+ " cloned by Reflection. Performance can be improved by adding the class to the list of known types");
							return newClone;
						}
					}
				}
				throw new Exception("Method clone not found");

				// ***************************************************************************************************************************************
				// 3. Polymorphic clone (Deep cloning by Serialization)
				// ***************************************************************************************************************************************
			} catch (Throwable e1) {
				try {
					ByteArrayOutputStream bytes = new ByteArrayOutputStream() {
						public synchronized byte[] toByteArray() {
							return buf;
						}
					};
					ObjectOutputStream out = new ObjectOutputStream(bytes);
					out.writeObject(objectToClone);
					out.close();
					ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
					System.out.println(objectToClone.getClass()
							+ " cloned by Serialization. Performance can be improved by adding the class to the list of known types");
					return in.readObject();

					// ***************************************************************************************************************************************
					// 4. Impossible to clone
					// ***************************************************************************************************************************************
				} catch (Throwable e2) {
					e2.printStackTrace();
					return null;
				}
			}
		}
	}
}