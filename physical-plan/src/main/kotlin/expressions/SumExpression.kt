package io.hqew.kquery.physical.expressions

class SumExpression(private val expr: Expression) : AggregateExpression {
    override fun inputExpression(): Expression {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return SumAccumulator()
    }

    override fun toString(): String {
        return "SUM($expr)"
    }
}

class SumAccumulator : Accumulator {
    private var value: Any? = 0
    override fun accumulate(value: Any?) {
        if (value != null) {
            when(value) {
                is Byte -> this.value = value + this.value as Byte
                is Short -> this.value = value + this.value as Short
                is Int -> this.value = value + this.value as Int
                is Long -> this.value = value + this.value as Long
                is Float -> this.value = value + this.value as Float
                is Double -> this.value = value + this.value as Double
                else -> throw UnsupportedOperationException("SUM does not support type of ${value.javaClass.name} ")
            }
        } else {
            throw IllegalStateException("SUM does not accept null value")
        }
    }

    override fun finalValue(): Any? {
        return this.value
    }
}
