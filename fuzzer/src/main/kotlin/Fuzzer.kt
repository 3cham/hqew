package io.hqew.kquery.fuzzer

import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.ArrowVectorBuilder
import io.hqew.kquery.datatypes.FieldVectorFactory
import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import kotlin.random.Random

class Fuzzer {
    private val rand = Random(0)

    private val enhancedRandom = EnhancedRandom(rand)

    /** Create a list of random values based on the provided data type */
    fun createValues(arrowType: ArrowType, n: Int): List<Any?> {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (0 until n).map{enhancedRandom.nextByte()}
            ArrowTypes.Int16Type -> (0 until n).map{enhancedRandom.nextShort()}
            ArrowTypes.Int32Type -> (0 until n).map{enhancedRandom.nextInt()}
            ArrowTypes.Int64Type -> (0 until n).map{enhancedRandom.nextLong()}
            ArrowTypes.FloatType -> (0 until n).map{enhancedRandom.nextFloat()}
            ArrowTypes.DoubleType -> (0 until n).map{enhancedRandom.nextDouble()}
            ArrowTypes.StringType -> (0 until n).map{enhancedRandom.nextString(rand.nextInt(64))}
            else -> throw UnsupportedOperationException("Unsupported createValues for $arrowType")
        }
    }

    /** Create a RecordBatch containing random data based on the provided schema */
    fun createRecordBatch(schema: Schema, n: Int): RecordBatch {
        val columns = schema.fields.map { it.dataType }.map { createValues(it, n) }
        return createRecordBatch(schema, columns)
    }

    private fun createRecordBatch(schema: Schema, column: List<List<Any?>>): RecordBatch {
        val arrowVectors = schema.fields.withIndex().map { field ->
            val fv = FieldVectorFactory.create(field.value.dataType, column[field.index].size)

            val arrowVector = ArrowVectorBuilder(fv)

            (0 until column[field.index].size).forEach {
                arrowVector.set(it, column[field.index][it])
            }

            arrowVector.setValueCount(column[field.index].size)
            arrowVector.build()
        }

        return RecordBatch(schema, arrowVectors)
    }
}

class EnhancedRandom(val rand: Random) {

    private val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')

    fun nextByte(): Byte {
        return when (rand.nextInt(5)) {
            0 -> Byte.MIN_VALUE
            1 -> Byte.MAX_VALUE
            2 -> -0
            3 -> 0
            4 -> rand.nextInt().toByte()
            else -> throw IllegalStateException()
        }
    }

    fun nextShort(): Short {
        return when (rand.nextInt(5)) {
            0 -> Short.MIN_VALUE
            1 -> Short.MAX_VALUE
            2 -> -0
            3 -> 0
            4 -> rand.nextInt().toShort()
            else -> throw IllegalStateException()
        }
    }

    fun nextInt(): Int {
        return when (rand.nextInt(5)) {
            0 -> Int.MIN_VALUE
            1 -> Int.MAX_VALUE
            2 -> -0
            3 -> 0
            4 -> rand.nextInt()
            else -> throw IllegalStateException()
        }
    }

    fun nextLong(): Long {
        return when (rand.nextInt(5)) {
            0 -> Long.MIN_VALUE
            1 -> Long.MAX_VALUE
            2 -> -0
            3 -> 0
            4 -> rand.nextLong()
            else -> throw IllegalStateException()
        }
    }

    fun nextDouble(): Double {
        return when (rand.nextInt(8)) {
            0 -> Double.MIN_VALUE
            1 -> Double.MAX_VALUE
            2 -> Double.POSITIVE_INFINITY
            3 -> Double.NEGATIVE_INFINITY
            4 -> Double.NaN
            5 -> -0.0
            6 -> 0.0
            7 -> rand.nextDouble()
            else -> throw IllegalStateException()
        }
    }

    fun nextFloat(): Float {
        return when (rand.nextInt(8)) {
            0 -> Float.MIN_VALUE
            1 -> Float.MAX_VALUE
            2 -> Float.POSITIVE_INFINITY
            3 -> Float.NEGATIVE_INFINITY
            4 -> Float.NaN
            5 -> -0.0f
            6 -> 0.0f
            7 -> rand.nextFloat()
            else -> throw IllegalStateException()
        }
    }

    fun nextString(len: Int): String {
        return (0 until len).map { _ -> rand.nextInt(charPool.size) }
            .map(charPool::get)
            .joinToString("")
    }
}