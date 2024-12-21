buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    idea
    `java-library`
    `maven-publish`
    id("com.github.johnrengelman.shadow") version "7.0.0"
}

version = "1.0"
group = "com.github.maujza.rabbitmq.spark"
description = "RabbitMQ Stream Connector"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

val scalaVersion = System.getProperty("scalaVersion", "2.12")
val sparkVersion = System.getProperty("sparkVersion", "3.4.1")

dependencies {
    compileOnly("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-streaming_$scalaVersion:$sparkVersion")
    implementation("org.json:json:20230618")
    implementation("com.rabbitmq:stream-client:0.15.0")
    shadow("org.json:json:20230618")
    shadow("com.rabbitmq:stream-client:0.15.0")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
    testImplementation("org.mockito:mockito-core:5.2.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.2.0")
    testImplementation("org.slf4j:slf4j-simple:2.0.9")
    testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.shadowJar {
    configurations = listOf(project.configurations.shadow.get())
}

tasks.register<Jar>("sourcesJar") {
    from(sourceSets.main.get().allSource)
    archiveClassifier.set("sources")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
