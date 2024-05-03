package io.hqew.kquery.logical

import io.hqew.kquery.datatypes.Schema

class Aggregate(
    val input: LogicalPlan, val groupExpr: List<LogicalExpr>, val aggregateExpr: List<AggregateExpr>
) : LogicalPlan {
    override fun schema(): Schema {
        return Schema( groupExpr.map{ it.toField(input)} + aggregateExpr.map { it.toField(input) } )
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Aggregate: groupExpr=$groupExpr, aggregateExpr=$aggregateExpr"
    }
}