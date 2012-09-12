/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.io.IOException;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;

public class OMultiFileSegment extends OSegment {
  protected OStorageSegmentConfiguration config;
  protected OFile[]                      files = new OFile[0];
  private final String                   fileExtension;
  private final String                   type;
  private final long                     maxSize;
  @SuppressWarnings("unused")
  private final String                   defrag;
  private int                            fileStartSize;
  final private int                      fileMaxSize;
  private final int                      fileIncrementSize;
  private boolean                        wasSoftlyClosedAtPreviousTime = true;

  public OMultiFileSegment(final OStorageLocal iStorage, final OStorageSegmentConfiguration iConfig, final String iFileExtension,
      final int iRoundMaxSize) throws IOException {
    super(iStorage, iConfig.name);

    config = iConfig;
    fileExtension = iFileExtension;
    type = iConfig.fileType;
    defrag = iConfig.defrag;
    maxSize = OFileUtils.getSizeAsNumber(iConfig.maxSize);
    fileStartSize = (int) OFileUtils.getSizeAsNumber(iConfig.fileStartSize);
    final int tmpFileMaxSize = (int) OFileUtils.getSizeAsNumber(iConfig.fileMaxSize);
    fileIncrementSize = (int) OFileUtils.getSizeAsNumber(iConfig.fileIncrementSize);

    if (iRoundMaxSize > 0)
      // ROUND THE FILE SIZE TO AVOID ERRORS ON ROUNDING BY DIVIDING FOR FIXED RECORD SIZE
      fileMaxSize = (tmpFileMaxSize / iRoundMaxSize) * iRoundMaxSize;
    else
      fileMaxSize = tmpFileMaxSize;
    // INSTANTIATE ALL THE FILES
    int perFileMaxSize;

    if (iConfig.infoFiles.length == 0) {
      // EMPTY FILE: CREATE THE FIRST FILE BY DEFAULT
      files = new OFile[1];
      files[0] = OFileFactory.instance().create(type,
          iStorage.getVariableParser().resolveVariables(config.getLocation() + "/" + name + "." + 0 + fileExtension),
          iStorage.getMode());
      perFileMaxSize = fileMaxSize;
      files[0].setMaxSize(perFileMaxSize);
      files[0].setIncrementSize(fileIncrementSize);

    } else {
      files = new OFile[iConfig.infoFiles.length];
      for (int i = 0; i < files.length; ++i) {
        files[i] = OFileFactory.instance().create(type, iStorage.getVariableParser().resolveVariables(iConfig.infoFiles[i].path),
            iStorage.getMode());
        perFileMaxSize = fileMaxSize;

        files[i].setMaxSize(perFileMaxSize);
        files[i].setIncrementSize(fileIncrementSize);
      }
    }
  }

  public void open() throws IOException {
    // @TODO: LAZY OPEN FILES
    for (OFile file : files)
      if (!file.open()) {
        // LAST TIME THE FILE WAS NOT CLOSED IN SOFT WAY
        OLogManager.instance().warn(this, "segment file '%s' was not closed correctly last time",
            OFileUtils.getPath(file.getName()));
        // TODO VERIFY DATA?
        wasSoftlyClosedAtPreviousTime = false;
      }
  }

  /**
   * Create the first file for current segment
   * 
   * @param iStartSize
   * @throws IOException
   */
  public void create(final int iStartSize) throws IOException {
    files = new OFile[1];
    fileStartSize = iStartSize;
    createNewFile();
  }

  public void close() throws IOException {
    for (OFile file : files) {
      if (file != null)
        file.close();
    }
  }

  public void delete() throws IOException {
    for (OFile file : files) {
      if (file != null)
        file.delete();
    }
  }

  public void truncate() throws IOException {
    // SHRINK TO 0
    files[0].shrink(0);

    if (files.length > 1) {
      // LEAVE JUST ONE FILE
      for (int i = 1; i < files.length; ++i) {
        if (files[i] != null)
          files[i].delete();
      }

      // UPDATE FILE STRUCTURE
      final OFile f = files[0];
      files = new OFile[1];
      files[0] = f;

      // UPDATE CONFIGURATION
      final OStorageFileConfiguration fileConfig = config.infoFiles[0];
      config.infoFiles = new OStorageFileConfiguration[1];
      config.infoFiles[0] = fileConfig;
      config.root.update();
    }
  }

  public void synch() throws IOException {
    for (OFile file : files) {
      if (file != null && file.isOpen())
        file.synch();
    }
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    for (OFile file : files) {
      if (file != null && file.isOpen())
        file.setSoftlyClosed(softlyClosed);
    }
  }

  public OStorageSegmentConfiguration getConfig() {
    return config;
  }

  public long getFilledUpTo() {
    long filled = 0;
    for (OFile file : files)
      filled += file.getFilledUpTo();

    return filled;
  }

  public long getSize() {
    long size = 0;
    for (OFile file : files)
      size += file.getFileSize();

    return size;
  }

  /**
   * Find free space for iRecordSize bytes.
   * 
   * @param iRecordSize
   * @return a pair file-id/file-pos
   * @throws IOException
   */
  protected long[] allocateSpace(final int iRecordSize) throws IOException {
    // IT'S PREFEREABLE TO FIND SPACE WITHOUT ENLARGE ANY FILES: FIND THE FIRST FILE WITH FREE SPACE TO USE
    OFile file;
    for (int i = 0; i < files.length; ++i) {
      file = files[i];

      if (file.getFreeSpace() >= iRecordSize)
        // FOUND: RETURN THIS OFFSET
        return new long[] { i, file.allocateSpace(iRecordSize) };
    }

    // NOT FOUND: CHECK IF CAN OVERSIZE SOME FILES
    for (int i = 0; i < files.length; ++i) {
      file = files[i];

      if (file.canOversize(iRecordSize)) {
        // FOUND SPACE: ENLARGE IT
        return new long[] { i, file.allocateSpace(iRecordSize) };
      }
    }

    // TRY TO CREATE A NEW FILE
    if (maxSize > 0 && getSize() >= maxSize)
      // OUT OF MAX SIZE
      throw new OStorageException("Unable to allocate the requested space of " + iRecordSize
          + " bytes because the segment is full: max-Size=" + maxSize + ", currentSize=" + getFilledUpTo());

    // COPY THE OLD ARRAY TO THE NEW ONE
    OFile[] newFiles = new OFile[files.length + 1];
    for (int i = 0; i < files.length; ++i)
      newFiles[i] = files[i];
    files = newFiles;

    // CREATE THE NEW FILE AND PUT IT AS LAST OF THE ARRAY
    file = createNewFile();
    file.allocateSpace(iRecordSize);

    config.root.update();

    return new long[] { files.length - 1, 0 };
  }

  /**
   * Return the absolute position receiving the pair file-id/file-pos.
   * 
   * @param iFilePosition
   *          as pair file-id/file-pos
   * @return
   */
  protected long getAbsolutePosition(final long[] iFilePosition) {
    long position = 0;
    for (int i = 0; i < iFilePosition[0]; ++i) {
      position += fileMaxSize;
    }
    return position + iFilePosition[1];
  }

  protected long[] getRelativePosition(final long iPosition) {
    if (iPosition < fileMaxSize)
      return new long[] { 0l, iPosition };

    final int fileNum = (int) (iPosition / fileMaxSize);

    if (fileNum >= files.length && fileNum < 0)
      throw new ODatabaseException("Record position #" + iPosition + " was bound to file #" + fileNum
          + " that is out of limit (files range 0-" + (files.length - 1) + ")");

    final int fileRec = (int) (iPosition % fileMaxSize);

    if (fileRec >= files[fileNum].getFilledUpTo() && fileRec < 0)
      throw new ODatabaseException("Record position #" + iPosition + " was bound to file #" + fileNum + " but the position #"
          + fileRec + " is out of file size " + files[fileNum].getFilledUpTo());

    return new long[] { fileNum, fileRec };
  }

  private OFile createNewFile() throws IOException {
    final int num = files.length - 1;

    final OFile file = OFileFactory.instance().create(type, config.getLocation() + "/" + name + "." + num + fileExtension,
        storage.getMode());
    file.setMaxSize(fileMaxSize);
    file.create(fileStartSize);
    files[num] = file;

    addInfoFileConfigEntry(file);

    return file;
  }

  private void addInfoFileConfigEntry(final OFile file) throws IOException {
    OStorageFileConfiguration[] newConfigFiles = new OStorageFileConfiguration[config.infoFiles.length + 1];
    for (int i = 0; i < config.infoFiles.length; ++i)
      newConfigFiles[i] = config.infoFiles[i];
    config.infoFiles = newConfigFiles;

    // CREATE A NEW ENTRY FOR THE NEW FILE
    String fileNameToStore = storage.getVariableParser().convertPathToRelative(OFileUtils.getPath(file.getPath()));

    final OStorageSegmentConfiguration template = config.root.fileTemplate;

    config.infoFiles[config.infoFiles.length - 1] = new OStorageFileConfiguration(config, fileNameToStore, template.fileType,
        template.fileMaxSize, template.fileIncrementSize);
  }

  public long allocateSpaceContinuously(final int iSize) throws IOException {
    // IT'S PREFERABLE TO FIND SPACE WITHOUT ENLARGE ANY FILES: FIND THE FIRST FILE WITH FREE SPACE TO USE
    OFile file;
    int remainingSize = iSize;
    // IF SOME FILES ALREADY CREATED
    int offset = -1;
    int fileNumber = -1;
    if (files.length > 0) {
      // CHECK IF THERE IS FREE SPACE IN LAST FILE IN CHAIN

      file = files[files.length - 1];

      if (file.getFreeSpace() > 0) {
        fileNumber = files.length - 1;
        if (remainingSize > file.getFreeSpace()) {
          remainingSize -= file.getFreeSpace();
          offset = file.allocateSpace(file.getFreeSpace());
        } else {
          return (long) (files.length - 1) * fileMaxSize + file.allocateSpace(remainingSize);
        }
      }

      // NOT FOUND FREE SPACE: CHECK IF CAN OVERSIZE LAST FILE

      final int oversize = fileMaxSize - file.getFileSize();
      if (oversize > 0 && remainingSize > 0) {
        fileNumber = files.length - 1;
        if (remainingSize > oversize) {
          remainingSize -= oversize;
          int newOffset = file.allocateSpace(oversize);
          // SAVE OFFSET IF IT WASN'T SAVED EARLIER
          if (offset == -1)
            offset = newOffset;
        } else {
          int newOffset = file.allocateSpace(remainingSize);
          if (offset == -1)
            offset = newOffset;
          if (fileNumber == -1) {
            fileNumber = files.length - 1;
          }
          return (long) fileNumber * fileMaxSize + offset;
        }
      }
    }

    // CREATE NEW FILE BECAUSE THERE IS NO FILES OR WE CANNOT ENLARGE EXISTING ENOUGH
    if (remainingSize > 0) {
      if (maxSize > 0 && getSize() >= maxSize)
        // OUT OF MAX SIZE
        throw new OStorageException("Unable to allocate the requested space of " + iSize
            + " bytes because the segment is full: max-Size=" + maxSize + ", currentSize=" + getFilledUpTo());

      // COPY THE OLD ARRAY TO THE NEW ONE
      OFile[] newFiles = new OFile[files.length + 1];
      for (int i = 0; i < files.length; ++i)
        newFiles[i] = files[i];
      files = newFiles;

      // CREATE THE NEW FILE AND PUT IT AS LAST OF THE ARRAY
      file = createNewFile();
      file.allocateSpace(iSize);

      config.root.update();

      if (fileNumber == -1) {
        fileNumber = files.length - 1;
      }

      if (offset == -1)
        offset = 0;
    }

    return (long) fileNumber * fileMaxSize + offset;
  }

  public void writeContinuously(long iPosition, byte[] iData) throws IOException {
    long[] pos = getRelativePosition(iPosition);

    // IT'S PREFERABLE TO FIND SPACE WITHOUT ENLARGE ANY FILES: FIND THE FIRST FILE WITH FREE SPACE TO USE
    OFile file;
    int remainingSize = iData.length;
    long offset = pos[1];

    for (int i = (int) pos[0]; remainingSize > 0; ++i) {
      file = files[i];
      if (remainingSize > file.getFilledUpTo() - offset) {
        if (file.getFilledUpTo() < offset) {
          throw new ODatabaseException("range check! " + file.getFilledUpTo() + " " + offset);
        }
        file.write(offset, iData, (int) (file.getFilledUpTo() - offset), iData.length - remainingSize);
        remainingSize -= (file.getFilledUpTo() - offset);
      } else {
        file.write(offset, iData, remainingSize, iData.length - remainingSize);
        remainingSize = 0;
      }
      offset = 0;
    }
  }

  public void readContinuously(final long iPosition, byte[] iBuffer, final int iSize) throws IOException {
    long[] pos = getRelativePosition(iPosition);

    // IT'S PREFERABLE TO FIND SPACE WITHOUT ENLARGE ANY FILES: FIND THE FIRST FILE WITH FREE SPACE TO USE
    OFile file;
    int remainingSize = iSize;
    long offset = pos[1];
    assert offset < Integer.MAX_VALUE;
    assert offset > -1;
    for (int i = (int) pos[0]; remainingSize > 0; ++i) {
      file = files[i];
      if (remainingSize > file.getFilledUpTo() - offset) {
        if (file.getFilledUpTo() < offset) {
          throw new ODatabaseException("range check! " + file.getFilledUpTo() + " " + offset);
        }
        int toRead = file.getFilledUpTo() - (int) offset;
        file.read(offset, iBuffer, toRead, iSize - remainingSize);
        remainingSize -= toRead;
      } else {
        file.read(offset, iBuffer, remainingSize, iSize - remainingSize);
        remainingSize = 0;
      }
      offset = 0;
    }
  }
    
  public boolean wasSoftlyClosedAtPreviousTime() {
    return wasSoftlyClosedAtPreviousTime;
  }
}
