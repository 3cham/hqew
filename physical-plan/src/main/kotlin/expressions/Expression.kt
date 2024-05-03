package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ColumnVector
import io.hqew.kquery.datatypes.RecordBatch

interface Expression {

    fun evaluate(input: RecordBatch): ColumnVector
}