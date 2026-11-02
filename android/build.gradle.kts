import java.util.zip.ZipFile

plugins {
    id("com.android.library") version "8.6.1"
    id("com.android.application") version "8.6.1" apply false
    kotlin("android") version "2.0.21"
    `maven-publish`
}

group = "io.synclite"
version = providers.gradleProperty("syncliteVersion").orElse("1.1.0").get()

val rustWorkspace = rootProject.projectDir.resolve("..")
val generatedJniLibs = layout.buildDirectory.dir("generated/jniLibs")
val skipAndroidRust = providers.gradleProperty("skipAndroidRust").map(String::toBoolean).orElse(false)
val androidAbis = listOf("arm64-v8a", "x86_64")
val androidApi = 26

android {
    namespace = "io.synclite.android"
    compileSdk = 35
    ndkVersion = "26.3.11579264"

    defaultConfig {
        minSdk = androidApi
        consumerProguardFiles("consumer-rules.pro")
        ndk {
            abiFilters += androidAbis
        }
    }

    externalNativeBuild {
        cmake {
            path = file("native/CMakeLists.txt")
            version = "3.22.1"
        }
    }

    sourceSets {
        getByName("main").jniLibs.srcDir(generatedJniLibs)
    }
}

kotlin {
    jvmToolchain(17)
}

tasks.register<Exec>("buildRustAndroidJni") {
    group = "build"
    description = "Builds the existing Rust JNI bridge for Android arm64-v8a and x86_64."
    workingDir = rustWorkspace
    // Rebuild whenever Rust sources change; without declared inputs Gradle
    // considers the task up to date and ships a stale native library.
    inputs.files(fileTree(rustWorkspace) {
        include("Cargo.toml", "Cargo.lock", "crates/**/*.rs", "crates/**/Cargo.toml", "native/**")
        exclude("target/**")
    })
    inputs.property("androidAbis", androidAbis)
    inputs.property("androidApi", androidApi)
    inputs.property("ndkVersion", android.ndkVersion)
    outputs.dir(generatedJniLibs)
    onlyIf {
        if (skipAndroidRust.get()) {
            logger.lifecycle("Skipping Android Rust JNI build because -PskipAndroidRust=true")
            false
        } else {
            true
        }
    }
    doFirst {
        // Keep cargo-ndk on the same NDK as externalNativeBuild, even when
        // ANDROID_NDK_HOME points to an older installation in the caller.
        environment("ANDROID_NDK_HOME", android.ndkDirectory.absolutePath)
    }
    environment("DUCKDB_DOWNLOAD_LIB", "false")
    commandLine(
        "cargo", "ndk",
        "-t", "arm64-v8a",
        "-t", "x86_64",
        "--platform", androidApi.toString(),
        "-o", generatedJniLibs.get().asFile.absolutePath,
        "build",
        "--release",
        "--package", "synclite-bindings-java"
    )
}

tasks.named("preBuild").configure {
    dependsOn("buildRustAndroidJni")
}

tasks.named<Delete>("clean").configure {
    delete(generatedJniLibs)
}

val verifyReleaseNativeLibraries = tasks.register("verifyReleaseNativeLibraries") {
    group = "verification"
    description = "Checks that the release AAR contains JNI and custom CDC libraries for each supported ABI."
    dependsOn("bundleReleaseAar")
    val aar = layout.buildDirectory.file("outputs/aar/${project.name}-release.aar")
    inputs.file(aar)
    doLast {
        ZipFile(aar.get().asFile).use { archive ->
            for (abi in androidAbis) {
                for (library in listOf("libsynclite_jni.so", "libsynclitecdc.so")) {
                    val name = "jni/$abi/$library"
                    val entry = checkNotNull(archive.getEntry(name)) { "Missing $name in release AAR" }
                    check(entry.size > 0) { "Empty $name in release AAR" }
                    archive.getInputStream(entry).use { stream ->
                        val magic = ByteArray(4)
                        check(stream.read(magic) == 4 && magic.contentEquals(byteArrayOf(0x7f, 0x45, 0x4c, 0x46))) {
                            "$name is not an ELF shared library"
                        }
                    }
                }
            }
        }
        logger.lifecycle("Verified Android JNI and custom CDC packaging for $androidAbis")
    }
}

tasks.matching { it.name == "assembleRelease" }.configureEach {
    dependsOn(verifyReleaseNativeLibraries)
}

publishing {
    val repositoryUrl = providers.gradleProperty("mavenRepoUrl").orNull
    if (!repositoryUrl.isNullOrBlank()) {
        repositories {
            maven {
                name = "maven"
                url = uri(repositoryUrl)
                credentials {
                    username = providers.gradleProperty("mavenUsername").orNull
                        ?: System.getenv("MAVEN_USERNAME")
                    password = providers.gradleProperty("mavenPassword").orNull
                        ?: System.getenv("MAVEN_PASSWORD")
                }
            }
        }
    }

    publications {
        register<MavenPublication>("release") {
            afterEvaluate {
                from(components["release"])
            }
            groupId = project.group.toString()
            artifactId = "synclite-android"
            version = project.version.toString()
        }
    }
}
