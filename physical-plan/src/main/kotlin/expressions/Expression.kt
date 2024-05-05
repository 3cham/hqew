package io.hqew.kquery.physical.expressions

import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.ColumnVector
import io.hqew.kquery.datatypes.LiteralValueVector
import io.hqew.kquery.datatypes.RecordBatch

interface Expression {
    fun evaluate(input: RecordBatch): ColumnVector
}

class LiteralLongExpression(val value: Long) : Expression{
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(
            arrowType = ArrowTypes.Int64Type,
            value = value,
            size = input.rowCount(),
        )
    }
}

class LiteralDoubleExpression(val value: Double) : Expression{
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(
            arrowType = ArrowTypes.DoubleType,
            value = value,
            size = input.rowCount(),
        )
    }
}

class LiteralStringExpression(val value: String) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(
            arrowType = ArrowTypes.StringType,
            value = value.toByteArray(),
            size = input.rowCount(),
        )
    }
}