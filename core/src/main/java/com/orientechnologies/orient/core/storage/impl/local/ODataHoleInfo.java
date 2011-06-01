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
package com.orientechnologies.orient.core.storage.impl.local;

/**
 * Keeps in memory the information about a hole in data segment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODataHoleInfo implements Comparable<ODataHoleInfo> {
	public int	size;
	public long	dataOffset;
	public int	holeOffset;

	public ODataHoleInfo() {
	}

	public ODataHoleInfo(final int iRecordSize, final long dataOffset, final int holeOffset) {
		this.size = iRecordSize;
		this.dataOffset = dataOffset;
		this.holeOffset = holeOffset;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (dataOffset ^ (dataOffset >>> 32));
		result = prime * result + holeOffset;
		result = prime * result + size;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ODataHoleInfo other = (ODataHoleInfo) obj;
		if (dataOffset != other.dataOffset)
			return false;
		if (holeOffset != other.holeOffset)
			return false;
		if (size != other.size)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return holeOffset + ") " + dataOffset + " [" + size + "]";
	}

	public int compareTo(final ODataHoleInfo o) {
		return size - o.size;
	}
}
