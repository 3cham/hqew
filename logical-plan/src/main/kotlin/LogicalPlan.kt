package io.hqew.kquery.logical

import io.hqew.kquery.datatypes.Schema

interface LogicalPlan {

    fun schema(): Schema

    fun children(): List<LogicalPlan>

    fun pretty(): String {
        return format(this)
    }
}

/** Format a logical plan in human-readable form**/
fun format(plan: LogicalPlan, indent: Int = 0): String {
    val b = StringBuffer()

    0.until(indent).forEach { _ -> b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach {
        b.append(format(it, indent + 1))
    }

    return b.toString()
}