package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ColumnVector
import io.hqew.kquery.datatypes.RecordBatch

abstract class BinaryExpression(val l: Expression, val r: Expression) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)

        // Must have same size
        assert(ll.size() == rr.size())

        if (ll.getType() != rr.getType()) {
            throw IllegalStateException("BinaryExpressions: only operands with same types supported, given ${ll.getType()} != ${rr.getType()}")
        }

        return evaluate(ll, rr)
    }

    abstract fun evaluate(ll: ColumnVector, rr: ColumnVector) : ColumnVector
}