package io.hqew.kquery.planner

import io.hqew.kquery.datasource.CsvDataSource
import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.Schema
import java.io.File

fun readEmployeeTestData(): CsvDataSource = CsvDataSource(
    File("../testdata", "employee.csv").absolutePath,
    schema = Schema(
        listOf(
            Field("id", ArrowTypes.Int64Type),
            Field("first_name", ArrowTypes.StringType),
            Field("last_name", ArrowTypes.StringType),
            Field("state", ArrowTypes.StringType),
            Field("job_title", ArrowTypes.StringType),
            Field("salary", ArrowTypes.DoubleType)
        )
    ), true, 1024
)