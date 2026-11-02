# SyncLite Android uses JNI methods resolved by their exported Rust symbol names.
# Keep the declaring class and its native method names when an app enables shrinking.
-keep class io.synclite.NativeConsolidator { *; }
-keep class io.synclite.SyncLiteException { *; }
