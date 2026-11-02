/*
 * Copyright (c) 2025 mahendra.chavan@syncLite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.synclite;

/**
 * Android declarations for the existing Rust JNI exports.
 *
 * <p>Android's package manager extracts the ABI-specific library from the
 * AAR, so this loader intentionally uses {@link System#loadLibrary(String)}
 * rather than the desktop JAR resource-extraction logic.</p>
 */
final class NativeConsolidator {
    private NativeConsolidator() {}

    static {
        System.loadLibrary("synclite_jni");
    }

    static native long nativeSpawnConsolidator(
            String dbPath,
            String workDir,
            String deviceDataRoot,
            String deviceId,
            String deviceName,
            String deviceType,
            String databaseName,
            String dstType,
            String dstConnectionString,
            String dstSyncMode,
            String dstDatabase,
            String dstSchema,
            String metadataStore,
            String stageDir,
            int dstIndex,
            int destinationCount,
            long devicePollingIntervalMs);

    static native void nativeNotifyStagePath(long handle, String stagePath);

    static native void nativeNotifyBootstrapReady(long handle, String backupPath, String metadataPath);

    static native void nativeCatchUpStageDir(long handle, String stageDir);

    static native void nativeStopConsolidator(long handle);

    static native void nativePauseSync(String dbPath);

    static native void nativeResumeSync(String dbPath);

    static native boolean nativeIsSyncPaused(String dbPath);

    static native void nativeReinitialize(String dbPath);

    static native void nativeAwaitSync(String dbPath, long timeoutMs);

    static native void nativeAwaitAppliedCommit(String dbPath, long targetCommitId, long timeoutMs);

    static native Object[] nativeSyncStatus(String dbPath);

    static native long[] nativeSyncStatistics(String dbPath);

    static native long[] nativeSyncLatency(String dbPath);
}
