plugins {
    java
    `java-library`
    kotlin("jvm") version "1.9.21" apply false

    id("com.diffplug.spotless") version "5.0.0"
}

group = "io.hqew.kquery"

version = "0.1.0-SNAPSHOT"

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
    }
}

subprojects {
    apply {
        plugin("org.jetbrains.kotlin.jvm")
        plugin("com.diffplug.spotless")
    }

    spotless { kotlin { ktfmt() } }

    extra["isReleaseVersion"] = !version.toString().endsWith("SNAPSHOT")

    val implementation by configurations
    val testImplementation by configurations

    dependencies {
        implementation(kotlin("stdlib-jdk8"))

        // Gradle plugin
        implementation(gradleApi())

        // Align versions of all Kotlin components
        implementation(platform("org.jetbrains.kotlin:kotlin-bom"))

        // Use the Kotlin JDK 8 standard library.
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

        // Use the Kotlin test library.
        testImplementation("org.jetbrains.kotlin:kotlin-test")

        // Use the Kotlin JUnit integration.
        testImplementation(platform("org.junit:junit-bom:5.10.2"))
        testImplementation("org.junit.jupiter:junit-jupiter")

        implementation(
                "org.jetbrains.kotlinx:kotlinx-serialization-runtime:0.20.0"
        ) // JVM dependency
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    }

    java { withJavadocJar() }
}
