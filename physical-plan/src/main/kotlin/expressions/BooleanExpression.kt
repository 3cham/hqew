package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ArrowFieldVector
import io.hqew.kquery.datatypes.ColumnVector
import io.hqew.kquery.datatypes.RecordBatch
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.types.pojo.ArrowType

abstract class BooleanExpression(val l: Expression, val r: Expression): Expression {

    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)

        assert(ll.size() == rr.size())

        if (ll.getType() != rr.getType()) {
            throw IllegalStateException("types of left and right operand in the expression must have same type")
        }

        return compare(ll, rr)
    }

    private fun compare(ll: ColumnVector, rr: ColumnVector): ColumnVector {
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()

        (0 until ll.size()).forEach {
            if (evaluate(ll.getValue(it), rr.getValue(it), ll.getType())) {
                v.set(it, 1)
            } else {
                v.set(it, 0)
            }

        }

        v.valueCount = ll.size()
        return ArrowFieldVector(v)
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean
}