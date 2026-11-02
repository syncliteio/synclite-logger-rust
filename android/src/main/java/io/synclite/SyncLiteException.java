/*
 * Copyright (c) 2025 mahendra.chavan@syncLite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.synclite;

/** Exception thrown when the Android JNI bridge reports a native SyncLite error. */
public final class SyncLiteException extends RuntimeException {
    public SyncLiteException(String message) {
        super(message);
    }

    public SyncLiteException(String message, Throwable cause) {
        super(message, cause);
    }
}
