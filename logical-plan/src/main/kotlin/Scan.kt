package io.hqew.kquery.logical

import io.hqew.kquery.datasource.DataSource
import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.Schema

class Scan(val path: String, val dataSource: DataSource, val projection: List<String>) : LogicalPlan{

    val schema = deriveSchema()

    private fun deriveSchema(): Schema {
        return if (projection.isEmpty())
            dataSource.schema()
        else
            dataSource.schema().select(projection)
    }

    override fun schema(): Schema {
        return schema
    }

    override fun children(): List<LogicalPlan> {
        return listOf()
    }

    override fun toString(): String {
        return if (projection.isEmpty())
            "Scan: $path; projection=None"
        else
            "Scan: $path; projection=$projection"
    }
}