package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.cloud.processor.CloudCommandProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupMode;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupModeConfig;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Enrico Risa on 19/12/2017.
 */
public abstract class AbstractBackupCommandProcessor implements CloudCommandProcessor {

  public static ODocument toODocument(BackupInfo info) {
    ODocument config = new ODocument();

    config.field("uuid", info.getUuid());
    config.field("dbName", info.getDbName());
    config.field("directory", info.getDirectory());
    config.field("enabled", info.getEnabled());
    config.field("retentionDays", info.getRetentionDays());

    Map<String, ODocument> modes = info.getModes().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().toString(), e -> new ODocument().field("when", e.getValue().getWhen())));

    config.field("modes", new ODocument().fromMap(modes));

    if (info.getUpload() != null)
      config.field("upload", new ODocument().fromMap(info.getUpload()));
    return config;
  }

  public static BackupInfo fromODocument(ODocument document) {
    BackupInfo info = new BackupInfo();
    info.setUuid(document.field("uuid"));
    info.setDbName(document.field("dbName"));
    info.setDirectory(document.field("directory"));
    info.setEnabled(document.field("enabled"));
    info.setRetentionDays(document.field("retentionDays"));
    info.setServer(document.field("server"));
    ODocument modes = document.field("modes");

    Iterable<Map.Entry<String, Object>> iterable = () -> modes.iterator();
    Map<BackupMode, BackupModeConfig> mappedModes = StreamSupport.stream(iterable.spliterator(), false)
        .collect(Collectors.toMap(e -> BackupMode.valueOf(e.getKey()), (e) -> {
          ODocument embedded = (ODocument) e.getValue();
          return new BackupModeConfig(embedded.field("when"));
        }));

    ODocument upload = document.field("upload");

    if (upload != null) {
      info.setUpload(upload.toMap());
    }
    info.setModes(mappedModes);
    return info;
  }
}
