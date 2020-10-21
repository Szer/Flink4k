import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    id("org.jetbrains.kotlin.jvm") version "1.4.10"
    id("org.jlleitschuh.gradle.ktlint") version "9.3.0"
    id("idea")
    `java-library`
}

repositories {
    jcenter()
}

group = "com.szer"

object V {
    const val scala = "2.12"
    const val flink = "1.11.1"
    const val kotlinx = "1.3.9"
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${V.kotlinx}")

    implementation("org.apache.flink:flink-streaming-java_${V.scala}:${V.flink}")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")

    testImplementation("org.apache.flink:flink-streaming-java_${V.scala}:${V.flink}:tests")
    testImplementation("org.apache.flink:flink-runtime_${V.scala}:${V.flink}:tests")
}

tasks {

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    test {
        testLogging {
            events(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)

            showExceptions = true
            exceptionFormat = TestExceptionFormat.FULL
            showCauses = true
            showStackTraces = true
            showExceptions = true
            exceptionFormat = TestExceptionFormat.FULL
            showCauses = true
            showStackTraces = true

            showStandardStreams = false
            showStandardStreams = false
        }
    }
}
