/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Keeps last search in thread local to be reused by further operations such as PUT. This speeds up work on trees. Saves the key,
 * the node and the position in the node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OMVRBTreeThreadLocal extends ThreadLocal<Map<OMVRBTree<?, ?>, Object[]>> {
	public static OMVRBTreeThreadLocal	INSTANCE	= new OMVRBTreeThreadLocal();

	@Override
	protected Map<OMVRBTree<?, ?>, Object[]> initialValue() {
		return new IdentityHashMap<OMVRBTree<?, ?>, Object[]>();
	}

	public synchronized <RET extends OMVRBTreeEntry<?, ?>> RET push(final OMVRBTree<?, ?> iTree, final Object iKey,
			final OMVRBTreeEntry<?, ?> iValue) {
		final Map<OMVRBTree<?, ?>, Object[]> map = get();
		Object[] value = map.get(iTree);

		if (value == null) {
			value = new Object[] { null, null };
			map.put(iTree, value);
		}

		value[0] = iKey;
		value[1] = iValue;
		// value[2] = iValue.tree.pageIndex;
		// value[3] = iValue.tree.pageItemFound;

		return (RET) iValue;
	}

	public synchronized Object[] search(final OMVRBTree<?, ?> iTree, final Object iKey) {
		final Map<OMVRBTree<?, ?>, Object[]> map = get();
		final Object[] value = map.get(iTree);

		if (value != null && value[0] != null && value[0].equals(iKey))
			return value;
		return null;
	}

	public synchronized void reset(final OMVRBTree<?, ?> iTree) {
		final Map<OMVRBTree<?, ?>, Object[]> map = get();
		final Object[] value = map.get(iTree);

		if (value != null) {
			value[0] = null;
			value[1] = null;
			// value[2] = -1;
			// value[3] = Boolean.FALSE;
		}
	}
}