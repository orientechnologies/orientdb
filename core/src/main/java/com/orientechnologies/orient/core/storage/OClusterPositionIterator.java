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
package com.orientechnologies.orient.core.storage;

import java.io.IOException;
import java.util.Iterator;

public class OClusterPositionIterator implements Iterator<Long> {
	private final OCluster	cluster;
	private long						current;
	private final long			max;

	public OClusterPositionIterator(final OCluster iCluster) throws IOException {
		cluster = iCluster;
		current = cluster.getFirstEntryPosition();
		max = cluster.getLastEntryPosition();
	}

	public OClusterPositionIterator(final OCluster iCluster, final long iBeginRange, final long iEndRange) throws IOException {
		cluster = iCluster;
		current = iBeginRange;
		max = iEndRange > -1 ? iEndRange : cluster.getLastEntryPosition();
	}

	public boolean hasNext() {
		return max > -1 && current <= max;
	}

	public Long next() {
		return current++;
	}

	public void remove() {
		throw new UnsupportedOperationException("remove");
	}
}
