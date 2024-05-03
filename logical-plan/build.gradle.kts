description = "hqew logical plan"


dependencies {
    implementation(project(":datasource"))
    implementation(project(":datatypes"))

    implementation("org.apache.arrow:arrow-memory:0.17.0")
    implementation("org.apache.arrow:arrow-vector:0.17.0")
}
