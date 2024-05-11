package io.hqew.kquery.physical.expressions

interface AggregateExpression {
    fun inputExpression(): Expression
    fun createAccumulator(): Accumulator
}

interface Accumulator{
    fun accumulate(value: Any?)
    fun finalValue() : Any?
}