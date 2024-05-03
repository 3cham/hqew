package io.hqew.kquery.physical

import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema

interface PhysicalPlan {

    fun execute(): Sequence<RecordBatch>

    fun schema(): Schema

    fun children(): List<PhysicalPlan>

    fun pretty(): String {
        return format(this)
    }
}

/** Format a physical plan in human-readable form */
private fun format(plan: PhysicalPlan, indent: Int = 0): String {
    val b = StringBuilder()
    0.until(indent).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, indent + 1)) }
    return b.toString()
}