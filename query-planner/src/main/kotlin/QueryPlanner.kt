package io.hqew.kquery.planner

import io.hqew.kquery.datatypes.Schema
import io.hqew.kquery.logical.*
import io.hqew.kquery.physical.*
import io.hqew.kquery.physical.expressions.ColumnExpression
import io.hqew.kquery.physical.expressions.Expression
import io.hqew.kquery.physical.expressions.LiteralDoubleExpression
import io.hqew.kquery.physical.expressions.LiteralLongExpression
import io.hqew.kquery.physical.expressions.LiteralStringExpression
import io.hqew.kquery.physical.expressions.MaxExpression
import io.hqew.kquery.physical.expressions.MinExpression
import io.hqew.kquery.physical.expressions.SumExpression

class QueryPlanner {
    fun createPhysicalPlan(plan: LogicalPlan): PhysicalPlan {
        // This is a placeholder for the actual implementation
        // that would convert a logical plan into a physical plan.
        // The implementation would depend on the structure of LogicalPlan
        // and how it maps to PhysicalPlan.
        return when(plan) {
            is Scan -> {
                ScanExec(plan.dataSource, plan.projection)
            }
            is Selection -> {
                val input = createPhysicalPlan(plan.input)
                val filterExpr = createPhysicalExpr(plan.expr, plan.input)
                SelectionExec(input, filterExpr)
            }
            is Projection -> {
                val input = createPhysicalPlan(plan.input)
                val projectionExpr = plan.expr.map { createPhysicalExpr(it, plan.input) }
                val projectionSchema = Schema(plan.expr.map { it.toField(plan.input) })
                ProjectionExec(input, projectionSchema, projectionExpr)
            }
            is Aggregate -> {
                val input = createPhysicalPlan(plan.input)
                val groupByExpr = plan.groupExpr.map { createPhysicalExpr(it, plan.input) }
                val aggregateExpr = plan.aggregateExpr.map {
                    when (it) {
                        is Max -> MaxExpression(createPhysicalExpr(it.expr, plan.input))
                        is Min -> MinExpression(createPhysicalExpr(it.expr, plan.input))
                        is Sum -> SumExpression(createPhysicalExpr(it.expr, plan.input))
//                        is Count -> CountExpression(createPhysicalExpr(it.expr, plan.input))
//                        is Avg -> AvgExpression(createPhysicalExpr(it.expr, plan.input))
//                        is CountDistinct -> CountDistinctExpression(createPhysicalExpr(it.expr, plan.input))
                        else -> throw UnsupportedOperationException("Aggregate function ${it::class.simpleName} is not supported yet.")
                    }
                }
                val aggregateSchema = plan.schema()
                HashAggregateExec(input, groupByExpr, aggregateExpr, aggregateSchema)
            }
            else -> {
                throw UnsupportedOperationException("Logical plan type ${plan::class.simpleName} is not supported yet.")
            }
        }
    }

    fun createPhysicalExpr(expr: LogicalExpr, input: LogicalPlan): Expression =
        when (expr) {
            is LiteralLong -> LiteralLongExpression(expr.n)
            is LiteralDouble -> LiteralDoubleExpression(expr.n)
            is LiteralString -> LiteralStringExpression(expr.str)
            is ColumnIndex -> ColumnExpression(expr.i)
            is Alias -> {
                createPhysicalExpr(expr.expr, input)
            }
            else -> throw UnsupportedOperationException("Logical expression type ${expr::class.simpleName} is not supported yet.")
        }
}