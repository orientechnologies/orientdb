package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.backup.log.OBackupLog;
import com.orientechnologies.agent.backup.log.OBackupLogType;
import com.orientechnologies.orientdb.cloud.protocol.backup.log.*;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Created by Enrico Risa on 21/12/2017.
 */
public class BackupLogConverter {

  static Map<OBackupLogType, Supplier<BackupLog>> builders;

  static {
    builders = new HashMap<>();
    builders.put(OBackupLogType.BACKUP_SCHEDULED, BackupScheduledLog::new);
    builders.put(OBackupLogType.BACKUP_STARTED, BackupStartedLog::new);
    builders.put(OBackupLogType.BACKUP_ERROR, BackupErrorLog::new);
    builders.put(OBackupLogType.BACKUP_FINISHED, BackupFinishedLog::new);
    builders.put(OBackupLogType.RESTORE_STARTED, RestoreStartedLog::new);
    builders.put(OBackupLogType.RESTORE_FINISHED, RestoreFinishedLog::new);
    builders.put(OBackupLogType.RESTORE_ERROR, RestoreErrorLog::new);
  }

  static BackupLog convert(OBackupLog input) {

    return bind(input, build(input));
  }

  private static BackupLog build(OBackupLog input) {
    return Optional.of(builders.get(input.getType())).map(e -> e.get())
        .orElseThrow(() -> new IllegalStateException("Cannot deserialize passed in log record."));
  }

  private static BackupLog bind(OBackupLog source, BackupLog target) {

    try {
      BeanUtils.copyProperties(target, source);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
    return target;
  }
}
