package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ColumnVector
import io.hqew.kquery.datatypes.RecordBatch


/**
 * The logical expression references a column name as string instead of its index
 * here we use the column's index to avoid the cost for column look up
 */
class ColumnExpression(val i: Int) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return input.field(i)
    }

    override fun toString(): String {
        return "#$i"
    }
}