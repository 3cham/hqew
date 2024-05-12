package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.ArrowVectorBuilder
import io.hqew.kquery.datatypes.ColumnVector
import io.hqew.kquery.datatypes.FieldVectorFactory
import org.apache.arrow.vector.types.pojo.ArrowType

abstract class MathExpression(l: Expression,  r: Expression) : BinaryExpression(l, r) {
    override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {

        val fieldVector = FieldVectorFactory.create(l.getType(), l.size())

        val builder = ArrowVectorBuilder(fieldVector)

        (0 until l.size()).forEach {
            builder.set(it, evaluate(l.getValue(it), r.getValue(it), l.getType()))
        }

        builder.setValueCount(l.size())
        return builder.build()
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any?
}

class AddExpression(l: Expression, r: Expression) : MathExpression(l, r) {

    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) + (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) + (r as Short)
            ArrowTypes.Int32Type -> (l as Int) + (r as Int)
            ArrowTypes.Int64Type -> (l as Long) + (r as Long)
            ArrowTypes.UInt8Type -> (l as Byte) + (r as Byte)
            ArrowTypes.UInt16Type -> (l as Short) + (r as Short)
            ArrowTypes.UInt32Type -> (l as Int) + (r as Int)
            ArrowTypes.UInt64Type -> (l as Long) + (r as Long)
            ArrowTypes.FloatType -> (l as Float) + (r as Float)
            ArrowTypes.DoubleType -> (l as Double) + (r as Double)
            else -> {
                throw IllegalStateException("Data type of $arrowType is not supported for addition expression")
            }
        }
    }

    override fun toString(): String {
        return "$l+$r"
    }
}

class SubtractExpression(l: Expression, r: Expression) : MathExpression(l, r) {

    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) - (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) - (r as Short)
            ArrowTypes.Int32Type -> (l as Int) - (r as Int)
            ArrowTypes.Int64Type -> (l as Long) - (r as Long)
            ArrowTypes.UInt8Type -> (l as Byte) - (r as Byte)
            ArrowTypes.UInt16Type -> (l as Short) - (r as Short)
            ArrowTypes.UInt32Type -> (l as Int) - (r as Int)
            ArrowTypes.UInt64Type -> (l as Long) - (r as Long)
            ArrowTypes.FloatType -> (l as Float) - (r as Float)
            ArrowTypes.DoubleType -> (l as Double) - (r as Double)
            else -> {
                throw IllegalStateException("Data type of $arrowType is not supported for subtraction expression")
            }
        }
    }

    override fun toString(): String {
        return "$l-$r"
    }
}

class MultiplyExpression(l: Expression, r: Expression) : MathExpression(l, r) {

    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) * (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) * (r as Short)
            ArrowTypes.Int32Type -> (l as Int) * (r as Int)
            ArrowTypes.Int64Type -> (l as Long) * (r as Long)
            ArrowTypes.UInt8Type -> (l as Byte) * (r as Byte)
            ArrowTypes.UInt16Type -> (l as Short) * (r as Short)
            ArrowTypes.UInt32Type -> (l as Int) * (r as Int)
            ArrowTypes.UInt64Type -> (l as Long) * (r as Long)
            ArrowTypes.FloatType -> (l as Float) * (r as Float)
            ArrowTypes.DoubleType -> (l as Double) * (r as Double)
            else -> {
                throw IllegalStateException("Data type of $arrowType is not supported for multiply expression")
            }
        }
    }

    override fun toString(): String {
        return "$l*$r"
    }
}

class DivideExpression(l: Expression, r: Expression) : MathExpression(l, r) {

    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) / (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) / (r as Short)
            ArrowTypes.Int32Type -> (l as Int) / (r as Int)
            ArrowTypes.Int64Type -> (l as Long) / (r as Long)
            ArrowTypes.UInt8Type -> (l as Byte) / (r as Byte)
            ArrowTypes.UInt16Type -> (l as Short) / (r as Short)
            ArrowTypes.UInt32Type -> (l as Int) / (r as Int)
            ArrowTypes.UInt64Type -> (l as Long) / (r as Long)
            ArrowTypes.FloatType -> (l as Float) / (r as Float)
            ArrowTypes.DoubleType -> (l as Double) / (r as Double)
            else -> {
                throw IllegalStateException("Data type of $arrowType is not supported for divide expression")
            }
        }
    }

    override fun toString(): String {
        return "$l/$r"
    }
}