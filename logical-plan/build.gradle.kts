description = "hqew logical plan"


dependencies {
    implementation(project(":datasource"))
    implementation(project(":datatypes"))

    implementation("org.apache.arrow:arrow-memory:16.0.0")
    implementation("org.apache.arrow:arrow-memory-netty:16.0.0")
    implementation("org.apache.arrow:arrow-vector:16.0.0")
}
