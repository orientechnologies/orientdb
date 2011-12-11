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
package com.orientechnologies.common.collection;

import java.util.ListIterator;

/**
 * Lazy iterator implementation based on List Iterator.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OLazyIteratorListWrapper<T> implements OLazyIterator<T> {
	private ListIterator<T>	underlying;

	public OLazyIteratorListWrapper(ListIterator<T> iUnderlying) {
		underlying = iUnderlying;
	}

	public boolean hasNext() {
		return underlying.hasNext();
	}

	public T next() {
		return underlying.next();
	}

	public void remove() {
		underlying.remove();
	}

	public T update(T e) {
		underlying.set(e);
		return null;
	}
}
