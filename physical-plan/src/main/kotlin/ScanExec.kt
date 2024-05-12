package io.hqew.kquery.physical

import io.hqew.kquery.datasource.DataSource
import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema

class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {
    override fun execute(): Sequence<RecordBatch> {
        return ds.scan(projection)
    }

    override fun schema(): Schema {
        return ds.schema().select(projection)
    }

    override fun children(): List<PhysicalPlan> {
        // Scan is a leaf node and has no children
        return listOf()
    }

    override fun toString(): String {
        return "Scan: schema=(${schema()}, projection=$projection"
    }
}