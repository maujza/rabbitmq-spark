import java.io.ByteArrayOutputStream
import java.net.URI

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    idea
    `java-library`
    `maven-publish`
    signing
    checkstyle
    id("com.github.gmazzo.buildconfig") version "3.0.2"
    id("com.github.spotbugs") version "4.7.9"
    id("com.github.johnrengelman.shadow") version "7.0.0"
}

version = "1.0"
group = "com.github.maujza"

description = "RabbitMQ Connector"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

// Usage: ./gradlew -DscalaVersion=2.12 -DsparkVersion=3.1.2
val scalaVersion = System.getProperty("scalaVersion", "2.13")
val sparkVersion = System.getProperty("sparkVersion", "3.2.2")

extra.apply {
    set("annotationsVersion", "22.0.0")
    set("sparkVersion", sparkVersion)
    set("scalaVersion", scalaVersion)
}

dependencies {
    compileOnly("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-streaming_$scalaVersion:$sparkVersion")
    implementation("org.json:json:20230618")
    implementation("com.rabbitmq:amqp-client:5.13.1")
    shadow("org.json:json:20230618")
    shadow("com.rabbitmq:amqp-client:5.13.1")

}

val defaultJdkVersion: Int = 11

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(defaultJdkVersion))
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(8)
}

tasks.shadowJar {
    configurations = listOf(project.configurations.shadow.get())
}

tasks.register<Jar>("sourcesJar") {
    description = "Create the sources jar"
    from(sourceSets.main.get().allSource)
    archiveClassifier.set("sources")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}


