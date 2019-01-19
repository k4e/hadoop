package org.apache.hadoop.yarn.api.protocolrecords;

public enum ContainerMigrationProcessType {
  PRE_CHECKPOINT,
  PRE_RESTORE,
  DO_CHECKPOINT,
  DO_RESTORE,
  POST_CHECKPOINT,
  POST_RESTORE,
  ABORT_CHECKPOINT,
  ABORT_RESTORE,
}
