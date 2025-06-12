package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ArrowFieldVector
import io.hqew.kquery.datatypes.ArrowTypes
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
            throw IllegalStateException("types of left and right operand in the expression must have same type: ${ll.getType()} != ${rr.getType()}")
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

class AndExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return toBool(l) && toBool(r)
    }
}

class OrExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return toBool(l) || toBool(r)
    }
}

class EqExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) == (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) == (r as Short)
            ArrowTypes.Int32Type -> (l as Int) == (r as Int)
            ArrowTypes.Int64Type -> (l as Long) == (r as Long)
            ArrowTypes.FloatType -> (l as Float) == (r as Float)
            ArrowTypes.DoubleType -> (l as Double) == (r as Double)
            ArrowTypes.StringType -> toString(l) == toString(r)
            else ->
                throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class NeqExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) != (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) != (r as Short)
            ArrowTypes.Int32Type -> (l as Int) != (r as Int)
            ArrowTypes.Int64Type -> (l as Long) != (r as Long)
            ArrowTypes.FloatType -> (l as Float) != (r as Float)
            ArrowTypes.DoubleType -> (l as Double) != (r as Double)
            ArrowTypes.StringType -> toString(l) != toString(r)
            else ->
                throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class LtExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) < (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) < (r as Short)
            ArrowTypes.Int32Type -> (l as Int) < (r as Int)
            ArrowTypes.Int64Type -> (l as Long) < (r as Long)
            ArrowTypes.FloatType -> (l as Float) < (r as Float)
            ArrowTypes.DoubleType -> (l as Double) < (r as Double)
            ArrowTypes.StringType -> toString(l) < toString(r)
            else ->
                throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class LtEqExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) <= (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) <= (r as Short)
            ArrowTypes.Int32Type -> (l as Int) <= (r as Int)
            ArrowTypes.Int64Type -> (l as Long) <= (r as Long)
            ArrowTypes.FloatType -> (l as Float) <= (r as Float)
            ArrowTypes.DoubleType -> (l as Double) <= (r as Double)
            ArrowTypes.StringType -> toString(l) <= toString(r)
            else ->
                throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class GtExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) > (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) > (r as Short)
            ArrowTypes.Int32Type -> (l as Int) > (r as Int)
            ArrowTypes.Int64Type -> (l as Long) > (r as Long)
            ArrowTypes.FloatType -> (l as Float) > (r as Float)
            ArrowTypes.DoubleType -> (l as Double) > (r as Double)
            ArrowTypes.StringType -> toString(l) > toString(r)
            else ->
                throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class GtEqExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) >= (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) >= (r as Short)
            ArrowTypes.Int32Type -> (l as Int) >= (r as Int)
            ArrowTypes.Int64Type -> (l as Long) >= (r as Long)
            ArrowTypes.FloatType -> (l as Float) >= (r as Float)
            ArrowTypes.DoubleType -> (l as Double) >= (r as Double)
            ArrowTypes.StringType -> toString(l) >= toString(r)
            else ->
                throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

private fun toString(v: Any?): String {
    return when (v) {
        is ByteArray -> String(v)
        else -> v.toString()
    }
}

private fun toBool(v: Any?): Boolean {
    when (v) {
        is Boolean -> return v == true
        is Number -> return v == 1
        else -> throw IllegalStateException()
    }
}