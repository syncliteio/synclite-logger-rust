/*
 * Copyright (c) 2025 mahendra.chavan@syncLite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.synclite;

/** JNI declarations for the SQLite-focused SyncLite Android runtime. */
final class NativeSqlite {
    private NativeSqlite() {}

    static {
        System.loadLibrary("synclite_jni");
    }

    static native void nativeInitialize(
            String deviceType,
            String deviceName,
            String dbPath,
            String destinationType,
            String destinationConnectionString,
            String destinationDatabase,
            String destinationSchema,
            String destinationSyncMode);

    static native long nativeOpen(String dbPath);

    static native long nativeConnectionInitialize(String dbPath);

    static native long nativeExecute(long handle, String sql, Object[] params);

    static native Object[][] nativeQuery(long handle, String sql, Object[] params);

    static native void nativeSetAutoCommit(long handle, boolean enabled);

    static native boolean nativeGetAutoCommit(long handle);

    static native void nativeCommit(long handle);

    static native void nativeRollback(long handle);

    static native void nativeFlush(long handle);

    static native void nativeClose(long handle);

    static native void nativeAwaitSync(String dbPath, long timeoutMs);
}
