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
package com.orientechnologies.orient.core.config;

import java.io.IOException;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OStorage;

@SuppressWarnings("serial")
public class OStorageConfiguration implements OSerializableStream {
	public static final ORecordId							CONFIG_RID			= new ORecordId(0, 0);

	public static final int										CURRENT_VERSION	= 2;

	public int																version					= -1;
	public String															name;
	public String															schemaRecordId;
	public String															dictionaryRecordId;
	public String															indexMgrRecordId;

	public String															localeLanguage	= Locale.getDefault().getLanguage();
	public String															localeCountry		= Locale.getDefault().getCountry();
	public String															dateFormat			= "yyyy-MM-dd";
	public String															dateTimeFormat	= "yyyy-MM-dd HH:mm:ss";

	public OStorageSegmentConfiguration				fileTemplate		= new OStorageSegmentConfiguration();

	public List<OStorageClusterConfiguration>	clusters				= new ArrayList<OStorageClusterConfiguration>();
	public List<OStorageDataConfiguration>		dataSegments		= new ArrayList<OStorageDataConfiguration>();

	public OStorageTxConfiguration						txSegment				= new OStorageTxConfiguration();

	public List<OStorageEntryConfiguration>		properties			= new ArrayList<OStorageEntryConfiguration>();

	private transient Locale									localeInstance;
	private transient DecimalFormatSymbols		unusualSymbols;
	protected transient OStorage							storage;

	public OStorageConfiguration(final OStorage iStorage) {
		storage = iStorage;
	}

	/**
	 * This method load the record information by the internal cluster segment. It's for compatibility with older database than
	 * 0.9.25.
	 * 
	 * @param iRequesterId
	 * 
	 * @compatibility 0.9.25
	 * @return
	 * @throws OSerializationException
	 */
	public OStorageConfiguration load() throws OSerializationException {
		final byte[] record = storage.readRecord(CONFIG_RID, null, null).buffer;

		if (record == null)
			throw new OStorageException("Cannot load database's configuration. The database seems to be corrupted.");

		fromStream(record);
		return this;
	}

	public void update() throws OSerializationException {
		final byte[] record = toStream();
		storage.updateRecord(CONFIG_RID, record, -1, ORecordBytes.RECORD_TYPE, null);
	}

	public boolean isEmpty() {
		return clusters.isEmpty();
	}

	public Locale getLocaleInstance() {
		if (localeInstance == null)
			localeInstance = new Locale(localeLanguage, localeCountry);

		return localeInstance;
	}

	public SimpleDateFormat getDateFormatInstance() {
		SimpleDateFormat dateFormatInstance = new SimpleDateFormat(dateFormat);
		dateFormatInstance.setLenient(false);
		return dateFormatInstance;
	}

	public SimpleDateFormat getDateTimeFormatInstance() {
		SimpleDateFormat dateTimeFormatInstance = new SimpleDateFormat(dateTimeFormat);
		dateTimeFormatInstance.setLenient(false);
		return dateTimeFormatInstance;
	}

	public DecimalFormatSymbols getUnusualSymbols() {
		if (unusualSymbols == null)
			unusualSymbols = new DecimalFormatSymbols(getLocaleInstance());
		return unusualSymbols;
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		final String[] values = new String(iStream).split("\\|");
		int index = 0;
		version = Integer.parseInt(read(values[index++]));

		name = read(values[index++]);

		schemaRecordId = read(values[index++]);
		dictionaryRecordId = read(values[index++]);

		if (version > 0)
			indexMgrRecordId = read(values[index++]);
		else
			// @COMPATIBILTY
			indexMgrRecordId = null;

		localeLanguage = read(values[index++]);
		localeCountry = read(values[index++]);
		dateFormat = read(values[index++]);
		dateTimeFormat = read(values[index++]);

		// @COMPATIBILTY
		if (version > 1)
			index = phySegmentFromStream(values, index, fileTemplate);

		int size = Integer.parseInt(read(values[index++]));
		String clusterType;
		int clusterId;
		String clusterName;

		// PREPARE THE LIST OF CLUSTERS
		clusters = new ArrayList<OStorageClusterConfiguration>(size);

		for (int i = 0; i < size; ++i) {
			clusterId = Integer.parseInt(read(values[index++]));

			if (clusterId == -1)
				continue;

			clusterName = read(values[index++]);

			clusterType = read(values[index++]);

			final OStorageClusterConfiguration currentCluster;

			if (clusterType.equals("p")) {
				// PHYSICAL CLUSTER
				final OStoragePhysicalClusterConfiguration phyCluster = new OStoragePhysicalClusterConfiguration(this, clusterId);
				phyCluster.name = clusterName;
				index = phySegmentFromStream(values, index, phyCluster);
				phyCluster.holeFile = new OStorageClusterHoleConfiguration(phyCluster, read(values[index++]), read(values[index++]),
						read(values[index++]));
				currentCluster = phyCluster;
			} else if (clusterType.equals("l"))
				// LOGICAL CLUSTER
				currentCluster = new OStorageLogicalClusterConfiguration(clusterName, clusterId, Integer.parseInt(read(values[index++])),
						new ORecordId(values[index++]));
			else
				// MEMORY CLUSTER
				currentCluster = new OStorageMemoryClusterConfiguration(clusterName, clusterId);

			// MAKE ROOMS, EVENTUALLY FILLING EMPTIES ENTRIES
			for (int c = clusters.size(); c <= clusterId; ++c)
				clusters.add(null);

			clusters.set(clusterId, currentCluster);
		}

		// PREPARE THE LIST OF DATA SEGS
		size = Integer.parseInt(read(values[index++]));
		dataSegments = new ArrayList<OStorageDataConfiguration>(size);
		for (int i = 0; i < size; ++i)
			dataSegments.add(null);

		int dataId;
		String dataName;
		OStorageDataConfiguration data;
		for (int i = 0; i < size; ++i) {
			dataId = Integer.parseInt(read(values[index++]));
			dataName = read(values[index++]);

			data = new OStorageDataConfiguration(this, dataName);
			index = phySegmentFromStream(values, index, data);
			data.holeFile = new OStorageDataHoleConfiguration(data, read(values[index++]), read(values[index++]), read(values[index++]));
			dataSegments.set(dataId, data);
		}

		txSegment = new OStorageTxConfiguration(read(values[index++]), read(values[index++]), read(values[index++]),
				read(values[index++]), read(values[index++]));

		size = Integer.parseInt(read(values[index++]));
		properties = new ArrayList<OStorageEntryConfiguration>(size);
		for (int i = 0; i < size; ++i) {
			properties.add(new OStorageEntryConfiguration(read(values[index++]), read(values[index++])));
		}

		return this;
	}

	public byte[] toStream() throws OSerializationException {
		final StringBuilder buffer = new StringBuilder();

		write(buffer, CURRENT_VERSION);
		write(buffer, name);

		write(buffer, schemaRecordId);
		write(buffer, dictionaryRecordId);
		write(buffer, indexMgrRecordId);

		write(buffer, localeLanguage);
		write(buffer, localeCountry);
		write(buffer, dateFormat);
		write(buffer, dateTimeFormat);

		phySegmentToStream(buffer, fileTemplate);

		write(buffer, clusters.size());
		for (OStorageClusterConfiguration c : clusters) {
			if (c == null) {
				write(buffer, -1);
				continue;
			}

			write(buffer, c.getId());
			write(buffer, c.getName());

			if (c instanceof OStoragePhysicalClusterConfiguration) {
				// PHYSICAL
				write(buffer, "p");
				phySegmentToStream(buffer, (OStoragePhysicalClusterConfiguration) c);
				fileToStream(buffer, ((OStoragePhysicalClusterConfiguration) c).holeFile);
			} else if (c instanceof OStorageLogicalClusterConfiguration) {
				// LOGICAL
				write(buffer, "l");
				logSegmentToStream(buffer, (OStorageLogicalClusterConfiguration) c);
			} else {
				// MEMORY
				write(buffer, "m");
			}
		}

		write(buffer, dataSegments.size());
		for (OStorageDataConfiguration d : dataSegments) {
			if (d == null)
				continue;

			write(buffer, d.id);
			write(buffer, d.name);

			phySegmentToStream(buffer, d);
			fileToStream(buffer, d.holeFile);
		}

		fileToStream(buffer, txSegment);
		write(buffer, txSegment.isSynchRecord());
		write(buffer, txSegment.isSynchTx());

		write(buffer, properties.size());
		for (OStorageEntryConfiguration e : properties)
			entryToStream(buffer, e);

		// PLAIN: ALLOCATE ENOUGHT SPACE TO REUSE IT EVERY TIME
		buffer.append("|");

		return buffer.toString().getBytes();
	}

	private int phySegmentFromStream(final String[] values, int index, final OStorageSegmentConfiguration iSegment) {
		iSegment.maxSize = read(values[index++]);
		iSegment.fileType = read(values[index++]);
		iSegment.fileStartSize = read(values[index++]);
		iSegment.fileMaxSize = read(values[index++]);
		iSegment.fileIncrementSize = read(values[index++]);
		iSegment.defrag = read(values[index++]);

		final int size = Integer.parseInt(read(values[index++]));
		iSegment.infoFiles = new OStorageFileConfiguration[size];
		String fileName;
		for (int i = 0; i < size; ++i) {
			fileName = read(values[index++]);

			if (!fileName.contains("$")) {
				// @COMPATIBILITY 0.9.25
				int pos = fileName.indexOf("/databases");
				if (pos > -1) {
					fileName = "${ORIENTDB_HOME}" + fileName.substring(pos);
				}
			}

			iSegment.infoFiles[i] = new OStorageFileConfiguration(iSegment, fileName, read(values[index++]), read(values[index++]),
					iSegment.fileIncrementSize);
		}

		return index;
	}

	private void phySegmentToStream(final StringBuilder iBuffer, final OStorageSegmentConfiguration iSegment) {
		write(iBuffer, iSegment.maxSize);
		write(iBuffer, iSegment.fileType);
		write(iBuffer, iSegment.fileStartSize);
		write(iBuffer, iSegment.fileMaxSize);
		write(iBuffer, iSegment.fileIncrementSize);
		write(iBuffer, iSegment.defrag);

		write(iBuffer, iSegment.infoFiles.length);
		for (OStorageFileConfiguration f : iSegment.infoFiles)
			fileToStream(iBuffer, f);
	}

	private void logSegmentToStream(final StringBuilder iBuffer, final OStorageLogicalClusterConfiguration iSegment) {
		write(iBuffer, iSegment.physicalClusterId);
		write(iBuffer, iSegment.map.toString());
	}

	private void fileToStream(final StringBuilder iBuffer, final OStorageFileConfiguration iFile) {
		write(iBuffer, iFile.path);
		write(iBuffer, iFile.type);
		write(iBuffer, iFile.maxSize);
	}

	private void entryToStream(final StringBuilder iBuffer, final OStorageEntryConfiguration iEntry) {
		write(iBuffer, iEntry.name);
		write(iBuffer, iEntry.value);
	}

	private String read(final String iValue) {
		if (iValue.equals(" "))
			return null;
		return iValue;
	}

	private void write(final StringBuilder iBuffer, final Object iValue) {
		if (iBuffer.length() > 0)
			iBuffer.append('|');
		iBuffer.append(iValue != null ? iValue.toString() : ' ');
	}

	public void close() throws IOException {
	}

	public void create() throws IOException {
		storage.createRecord(CONFIG_RID, new byte[] { 0, 0, 0, 0 }, ORecordBytes.RECORD_TYPE, null);
	}
}
