description = "hqew data sources"


dependencies {
    implementation(project(":datatypes"))

    implementation("org.apache.hadoop:hadoop-common:3.4.0")
    implementation("org.apache.parquet:parquet-arrow:1.13.1")
    implementation("org.apache.parquet:parquet-common:1.13.1")
    implementation("org.apache.parquet:parquet-column:1.13.1")
    implementation("org.apache.parquet:parquet-hadoop:1.13.1")

    implementation("com.univocity:univocity-parsers:2.9.1")
}
