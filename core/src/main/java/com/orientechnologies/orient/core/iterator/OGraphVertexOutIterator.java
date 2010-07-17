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
package com.orientechnologies.orient.core.iterator;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Stack;

import com.orientechnologies.orient.core.db.graph.OGraphVertex;

public class OGraphVertexOutIterator implements Iterator<OGraphVertex>, Iterable<OGraphVertex> {
	private OGraphVertex				current;
	private Stack<OGraphVertex>	stack							= new Stack<OGraphVertex>();
	private int									count							= 0;
	private int									currentDeepLevel	= 0;
	private int									maxDeepLevel			= 0;

	public OGraphVertexOutIterator(final OGraphVertex iRoot) {
		current = iRoot;
	}

	public boolean hasNext() {
		return current != null;
	}

	public OGraphVertex next() {
		final OGraphVertex result = current;

		if (current != null) {
			if (current.hasOutEdges()) {
				// GO IN DEEP
				stack.push(current.clone());

				// SET THE FIRST LINK AS CURRENT
				current = current.getOutEdgeVertex(0, current);
				currentDeepLevel++;

				if (currentDeepLevel > maxDeepLevel)
					maxDeepLevel = currentDeepLevel;
			} else {
				// GO BACK RECURSIVELY UNTIL FIND A GOOD VERTEX
				int prevPosition;
				OGraphVertex previous;

				while (!stack.isEmpty()) {
					previous = stack.peek();
					prevPosition = previous.findOutVertex(current);

					currentDeepLevel--;

					if (prevPosition == -1)
						// VERTEX REMOVED CONCURRENTLY
						throw new ConcurrentModificationException("Vertex " + previous + " has been removed while iterating");

					if (prevPosition < previous.getOutEdgeCount() - 1) {
						current = previous.getOutEdgeVertex(prevPosition + 1, current);
						break;
					}

					current = previous;
					stack.pop();
				}
			}
		}

		count++;

		return result;
	}

	public void remove() {
		throw new UnsupportedOperationException("remove");
	}

	public Iterator<OGraphVertex> iterator() {
		return this;
	}

	public int getCurrentDeepLevel() {
		return currentDeepLevel;
	}

	public int getMaxDeepLevel() {
		return maxDeepLevel;
	}

	public int getCount() {
		return count;
	}
}
