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

/**
 * Keeps last search in thread local to be reused by further operations such as PUT. This speeds up work on trees. Saves the key,
 * the node and the position in the node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OMVRBTreeThreadLocal extends ThreadLocal<Object[]> {
	public static OMVRBTreeThreadLocal	INSTANCE	= new OMVRBTreeThreadLocal();

	@Override
	protected Object[] initialValue() {
		return new Object[] { null, null };
	}

	public <RET extends OMVRBTreeEntry<?, ?>> RET push(final Object iKey, final OMVRBTreeEntry<?, ?> iValue) {
		final Object[] value = get();
		value[0] = iKey;
		value[1] = iValue;
		return (RET) iValue;
	}

	public Object[] search(final Object key) {
		final Object[] value = get();
		if (value != null && value[0] != null && value[0].equals(key))
			return value;
		return null;
	}

	public void reset() {
		final Object[] value = get();
		value[0] = null;
		value[1] = null;
	}

	public <RET extends OMVRBTreeEntry<?, ?>> RET getLatest() {
		final Object[] value = get();
		if (value != null && value[1] != null)
			return (RET) value[1];
		return null;
	}
}