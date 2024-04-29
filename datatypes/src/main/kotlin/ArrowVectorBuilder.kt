package io.hqew.kquery.datatypes

import java.lang.IllegalStateException
import org.apache.arrow.vector.*

class ArrowVectorBuilder(val fieldVector: FieldVector) {
    fun set(i: Int, value: Any?) {
        when (fieldVector) {
            is VarCharVector -> {
                when (value) {
                    null -> fieldVector.setNull(i)
                    is ByteArray -> fieldVector.set(i, value)
                    else -> fieldVector.set(i, value.toString().toByteArray())
                }
            }
            is TinyIntVector -> {
                when (value) {
                    null -> fieldVector.setNull(i)
                    is Number -> fieldVector.set(i, value.toByte())
                    is String -> fieldVector.set(i, value.toByte())
                    else -> throw IllegalStateException()
                }
            }
            is SmallIntVector -> {
                when (value) {
                    null -> fieldVector.setNull(i)
                    is Number -> fieldVector.set(i, value.toShort())
                    is String -> fieldVector.set(i, value.toShort())
                    else -> throw IllegalStateException()
                }
            }
            is IntVector -> {
                when (value) {
                    null -> fieldVector.setNull(i)
                    is Number -> fieldVector.set(i, value.toInt())
                    is String -> fieldVector.set(i, value.toInt())
                    else -> throw IllegalStateException()
                }
            }
            is BigIntVector -> {
                when (value) {
                    null -> fieldVector.setNull(i)
                    is Number -> fieldVector.set(i, value.toLong())
                    is String -> fieldVector.set(i, value.toLong())
                    else -> throw IllegalStateException()
                }
            }
            is Float4Vector -> {
                when (value) {
                    null -> fieldVector.setNull(i)
                    is Number -> fieldVector.set(i, value.toFloat())
                    is String -> fieldVector.set(i, value.toFloat())
                    else -> throw IllegalStateException()
                }
            }
            is Float8Vector -> {
                when (value) {
                    null -> fieldVector.setNull(i)
                    is Number -> fieldVector.set(i, value.toDouble())
                    is String -> fieldVector.set(i, value.toDouble())
                    else -> throw IllegalStateException()
                }
            }
            else -> throw IllegalStateException(fieldVector.javaClass.name)
        }
    }

    fun setValueCount(n: Int) {
        fieldVector.valueCount = n
    }

    fun build(): ColumnVector {
        return ArrowFieldVector(fieldVector)
    }
}
