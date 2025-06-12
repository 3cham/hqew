package io.hqew.kquery.physical

import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema
import io.hqew.kquery.physical.expressions.Accumulator
import io.hqew.kquery.physical.expressions.AggregateExpression
import io.hqew.kquery.physical.expressions.Expression

class HashAggregateExec (
    val input: PhysicalPlan,
    val groupExpr: List<Expression>,
    val aggregateExpr: List<AggregateExpression>,
    val schema: Schema
) : PhysicalPlan {

    // evaluate all aggregateExpr on all batches
    // track a hashmap of group expr
    override fun execute(): Sequence<RecordBatch> {
        val input = input.execute()

        val valMap = HashMap<List<Any?>, List<Accumulator>>()

        input.iterator().forEach { batch ->
            val groupKeys = groupExpr.map{ it.evaluate(batch) }

            val aggInputValues = aggregateExpr.map{
                it.inputExpression().evaluate(batch)
            }

            (0 until batch.rowCount()).forEach { rowIndex ->
                val rowKey = groupKeys.map{ it.getValue(rowIndex) }
                TODO()
            }
        }

        TODO()
    }

    override fun schema(): Schema {
        return schema
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "HashAggregateExec: groupExpr=$groupExpr, aggregateExpr=$aggregateExpr"
    }
}