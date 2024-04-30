package io.hqew.kquery.datasource

import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema

interface DataSource {

    fun schema(): Schema

    fun scan(projection: List<String>): Sequence<RecordBatch>
}