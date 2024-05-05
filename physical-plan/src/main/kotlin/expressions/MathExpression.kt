package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ArrowVectorBuilder
import io.hqew.kquery.datatypes.ColumnVector
import org.apache.arrow.vector.types.pojo.ArrowType

abstract class MathExpression(l: Expression,  r: Expression) : BinaryExpression(l, r) {
    override fun evaluate(ll: ColumnVector, rr: ColumnVector): ColumnVector {

        // val builder: ArrowVectorBuilder()
        TODO()
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any?
}