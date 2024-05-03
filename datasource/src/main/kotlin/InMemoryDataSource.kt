package io.hqew.kquery.datasource

import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema

class InMemoryDataSource(
    val schema: Schema,
    val data: List<RecordBatch>
) : DataSource {
    override fun schema(): Schema {
        return schema
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        val newSchema = data.first().schema.select(projection)

        val fields = projection.map { column -> schema.fields.indexOfFirst { f -> f.name == column } }

        return data.asSequence().map{ batch ->
            RecordBatch(
                newSchema,
                fields.map{ i -> batch.field(i) }
            )
        }
    }
}