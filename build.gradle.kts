import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.4.10"
    kotlin("plugin.serialization") version "2.4.10"
    id("io.gitlab.arturbosch.detekt") version "1.23.8"
    id("org.jetbrains.dokka") version "2.2.0"
    id("org.jetbrains.kotlinx.kover") version "0.9.9"
    id("ru.vyarus.animalsniffer") version "2.0.1"
    `maven-publish`
}

group = "io.hafa"
version = "0.7.0"

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
}

kotlin {
    explicitApi()

    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        // Emit StringBuilder concatenation rather than invokedynamic/StringConcatFactory,
        // which doesn't exist below Android 34. D8 desugars it, but generating it inline
        // keeps the bytecode itself free of post-API-21 references so animalsniffer can
        // actually verify the Android floor instead of trusting the toolchain.
        freeCompilerArgs.add("-Xstring-concat=inline")
        // a warning the build tolerates is a warning nobody reads
        allWarningsAsErrors.set(true)
    }
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    // api, not implementation: SessionOptions.httpClient exposes OkHttpClient publicly.
    api("com.squareup.okhttp3:okhttp:5.5.0")
    implementation("com.squareup.okhttp3:okhttp-coroutines:5.5.0")

    // Android API-21 signatures; the automated check behind the minSdk-21 claim.
    signature("com.toasttab.android:gummy-bears-api-21:0.15.0@signature")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testImplementation("com.squareup.okhttp3:mockwebserver3:5.5.0")
    testImplementation("com.squareup.okhttp3:mockwebserver3-junit5:5.5.0")
}

tasks.test {
    useJUnitPlatform()
}

// Verify only our own classes against the Android signatures; dependencies ship their
// own compatibility guarantees and would drown the report.
animalsniffer {
    sourceSets = listOf(java.sourceSets.main.get())
}

// Full default rule set plus a public-API doc gate; deviations in config/detekt/detekt.yml.
// Tests are linted too: they are the larger half of the codebase and the place a stale
// assertion hides, so exempting them would exempt most of what there is to get wrong.
detekt {
    source.setFrom("src/main/kotlin", "src/test/kotlin")
    config.setFrom("$projectDir/config/detekt/detekt.yml")
    buildUponDefaultConfig = true
}

kover {
    reports {
        verify {
            rule {
                bound {
                    minValue = 90
                }
            }
        }
    }
}

// detekt's comments ruleset gates whether a public declaration is documented, but nothing in
// it gates the shape. A KDoc that opens with a paragraph has no summary for Dokka to show in
// an index, so a multi-line one must lead with a single line and then break.
val checkKdocSummaries by tasks.registering {
    val sources = fileTree("src/main/kotlin") { include("**/*.kt") }
    inputs.files(sources)
    outputs.upToDateWhen { true }
    doLast {
        val offences = sources.files.sorted().flatMap { file ->
            val lines = file.readLines()
            val bad = mutableListOf<String>()
            var index = 0
            while (index < lines.size) {
                val opener = lines[index].trim()
                if (opener.startsWith("/**") && !opener.endsWith("*/")) {
                    val body = lines.asSequence()
                        .drop(index + 1)
                        .takeWhile { it.trim() != "*/" }
                        .map { it.trim() }
                        .toList()
                    if (body.size >= 2 && body[1] != "*") {
                        bad += "${file.path}:${index + 1}: multi-line KDoc without a one-line " +
                            "summary followed by a blank line"
                    }
                    index += body.size + 1
                }
                index++
            }
            bad
        }
        if (offences.isNotEmpty()) {
            throw GradleException(offences.joinToString("\n", prefix = "\n"))
        }
    }
}

tasks.check {
    dependsOn(checkKdocSummaries)
    dependsOn(tasks.named("koverVerify"))
    // so a broken KDoc link fails CI, which runs `build` and nothing else
    dependsOn(tasks.named("dokkaGeneratePublicationHtml"))
}

// Bundles Dokka's HTML as a javadoc-classified jar; JitPack serves it at javadoc.jitpack.io.
val dokkaJavadocJar = tasks.register<Jar>("dokkaJavadocJar") {
    dependsOn(tasks.named("dokkaGeneratePublicationHtml"))
    from(layout.buildDirectory.dir("dokka/html"))
    archiveClassifier.set("javadoc")
}

// An unresolved KDoc link is a broken cross-reference in the published docs, so it fails
// the build rather than scrolling past in the log.
dokka {
    dokkaPublications.configureEach {
        failOnWarning.set(true)
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "rmapi-kt"
            from(components["java"])
            artifact(dokkaJavadocJar)
            pom {
                name.set("rmapi-kt")
                description.set("Kotlin/JVM client for the reMarkable cloud API")
                url.set("https://github.com/hafacc/rmapi-kt")
                licenses {
                    license {
                        name.set("MIT")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
                developers {
                    developer {
                        id.set("erikbrinkman")
                        name.set("Erik Brinkman")
                        email.set("erik.brinkman@gmail.com")
                    }
                }
                scm {
                    url.set("https://github.com/hafacc/rmapi-kt")
                    connection.set("scm:git:https://github.com/hafacc/rmapi-kt.git")
                    developerConnection.set("scm:git:git@github.com:hafacc/rmapi-kt.git")
                }
            }
        }
    }
}
