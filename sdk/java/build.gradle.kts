import org.gradle.api.tasks.wrapper.Wrapper

plugins {
	`java-library`
	`maven-publish`
	alias(libs.plugins.lombok)
}

group = "io.github.lyonbrown4d"
version = "0.1.0-SNAPSHOT"

java {
	sourceCompatibility = JavaVersion.VERSION_21
	targetCompatibility = JavaVersion.VERSION_21
	withSourcesJar()
}

dependencies {
	implementation(libs.netty.buffer)
	implementation(libs.netty.codec)
	implementation(libs.netty.handler)
	implementation(libs.netty.transport)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(21)
}

tasks.wrapper {
	gradleVersion = "9.5.1"
	distributionType = Wrapper.DistributionType.BIN
	distributionUrl = "https://downloads.gradle.org/distributions/gradle-9.5.1-bin.zip"
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "nespa-java"
        }
    }
}
