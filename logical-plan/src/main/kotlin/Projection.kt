package io.hqew.kquery.logical

import io.hqew.kquery.datatypes.Schema

class Projection(val input: LogicalPlan, val expr: List<LogicalExpr>) : LogicalPlan {
    override fun schema(): Schema {
        return Schema(expr.map { it.toField(input) })
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Projection: ${expr.joinToString(", ") { it.toString() }}"
    }
}