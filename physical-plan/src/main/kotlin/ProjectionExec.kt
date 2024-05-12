package io.hqew.kquery.physical

import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema
import io.hqew.kquery.physical.expressions.Expression

class ProjectionExec(
    val input: PhysicalPlan,
    val schema: Schema,
    val expr: List<Expression>,
) : PhysicalPlan {

    override fun execute(): Sequence<RecordBatch> {
        return input.execute().map { batch ->
            val columns = expr.map{ it.evaluate(batch) }
            RecordBatch(schema, columns)
        }
    }

    override fun schema(): Schema {
        return schema
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "ProjectionExec: $expr"
    }
}