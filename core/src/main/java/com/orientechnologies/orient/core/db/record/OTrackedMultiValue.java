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
package com.orientechnologies.orient.core.db.record;

import java.util.List;

/**
 * Interface that indicates that collection will send notifications about operations that are performed on it to the listeners.
 * 
 * @param <K>
 *          Value that indicates position of item inside collection.
 * @param <V>
 *          Value that is hold by collection.
 */
public interface OTrackedMultiValue<K, V> {

	/**
	 * Add change listener.
	 * 
	 * @param changeListener
	 *          Change listener instance.
	 */
	public void addChangeListener(OMultiValueChangeListener<K, V> changeListener);

	/**
	 * Remove change listener.
	 * 
	 * @param changeListener
	 *          Change listener instance.
	 */
	public void removeRecordChangeListener(OMultiValueChangeListener<K, V> changeListener);

	/**
	 * 
	 * Reverts all operations that were performed on collection and return original collection state.
	 * 
	 * @param changeEvents
	 *          List of operations that were performed on collection.
	 * 
	 * @return Original collection state.
	 */
	public Object returnOriginalState(List<OMultiValueChangeEvent<K, V>> changeEvents);

	public Class<?> getGenericClass();
}
