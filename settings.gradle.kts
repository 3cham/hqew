plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}
rootProject.name = "hqew"

// hqew JVM Query Engine

include("datatypes")
include("datasource")
include("logical-plan")
