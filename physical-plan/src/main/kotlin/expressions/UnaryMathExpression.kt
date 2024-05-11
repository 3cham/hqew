package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ArrowFieldVector
import io.hqew.kquery.datatypes.ColumnVector
import io.hqew.kquery.datatypes.RecordBatch
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import kotlin.math.ln
import kotlin.math.sqrt

abstract class UnaryMathExpression(private val expr : Expression) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val n = evaluate(input)
        val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()

        (0 until n.size()).forEach {
            val nv = n.getValue(it)
            if (nv == null) {
                v.setNull(it)
            } else if (nv is Double){
                v.set(it, apply(nv))
            } else {
                TODO()
            }
        }

        return ArrowFieldVector(v)
    }

    abstract fun apply(value: Double) : Double
}

class Sqrt(expr: Expression): UnaryMathExpression(expr) {
    override fun apply(value: Double): Double {
        return sqrt(value)
    }
}

class Log(expr: Expression): UnaryMathExpression(expr) {
    override fun apply(value: Double): Double {
        return ln(value)
    }
}