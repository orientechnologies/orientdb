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
package com.orientechnologies.orient.core.record;

import java.util.Iterator;

/**
 * Generic record representation without a schema definition. The object can be reused across call to the database.
 */
public interface ORecordPositional<T> extends ORecordInternal<T>, Iterator<T> {
	public <E> E field(int iIndex);

	public ORecordPositional<T> field(int iIndex, Object iValue);

	public int size();

	public ORecordPositional<T> add(Object iValue);
}
