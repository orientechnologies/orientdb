/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.index;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Key that is used in {@link com.orientechnologies.orient.core.index.mvrbtree.OMVRBTree} for partial composite key search.
 * It always greater than any passed in key.
 *
 * @author Andrey Lomakin
 * @since 20.03.12
 */
@SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
public final class OAlwaysGreaterKey implements Comparable<Comparable<?>>{
	public int compareTo(Comparable<?> o) {
		return 1;
	}
}
