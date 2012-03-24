/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.common.collection;

/**
 * Key that is used in {@link OMVRBTree} for partial composite key search.
 * It always lesser than any passed in key.
 *
 * @author Andrey Lomakin
 * @since 20.03.12
 */
public final class OAlwaysLessKey implements Comparable<Comparable> {
	public int compareTo(Comparable o) {
		return -1;
	}
}