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

/**
 * Handle the table to resolve logical address to physical address.<br/>
 * <br/>
 * Record structure:<br/>
 * <br/>
 * +---------------------------------------------+<br/>
 * | DATA SEGMENT........ | DATA OFFSET......... |<br/>
 * | 2 bytes = max 2^15-1 | 4 bytes = max 2^31-1 |<br/>
 * +---------------------------------------------+<br/>
 * = 6 bytes<br/>
 */
public interface OCluster {

	public void create(int iStartSize) throws IOException;

	public void open() throws IOException;

	public void close() throws IOException;

	public void delete() throws IOException;

	public String getType();

	/**
	 * Add a new entry.
	 */
	public long addPhysicalPosition(int iDataSegmentId, long iPosition, final byte iRecordType) throws IOException;

	/**
	 * Fill and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
	 * 
	 * @throws IOException
	 */
	public OPhysicalPosition getPhysicalPosition(long iPosition, OPhysicalPosition iPPosition) throws IOException;

	/**
	 * Change the PhysicalPosition of the logical record iPosition.
	 */
	public void setPhysicalPosition(long iPosition, int iDataSegment, long iDataPosition, final byte iRecordType) throws IOException;

	/**
	 * Remove the Logical Position entry.
	 */
	public void removePhysicalPosition(long iPosition, OPhysicalPosition iPPosition) throws IOException;

	public void updateRecordType(long iPosition, final byte iRecordType) throws IOException;

	public void updateVersion(long iPosition, int iVersion) throws IOException;

	public long getEntries() throws IOException;

	public long getFirstEntryPosition() throws IOException;

	public long getLastEntryPosition() throws IOException;

	/**
	 * Let to an external actor to lock the cluster in shared mode. Useful for range queries to avoid atomic locking.
	 * 
	 * @see #unlock();
	 */
	public void lock();

	/**
	 * Let to an external actor to unlock the shared mode lock acquired by the lock().
	 * 
	 * @see #lock();
	 */
	public void unlock();

	public int getId();

	public void synch();

	public String getName();

	public OClusterPositionIterator absoluteIterator() throws IOException;
}
