package io.hqew.kquery.logical

import io.hqew.kquery.datatypes.Field

/**
 * Logical Expression for use in logical query plans. The logical expression provides information
 * needed during the planing phase such as the name and data type of the expression.
 */
interface LogicalExpr {

    /**
     * Return metadata about the value that will be produced by this expression when
     * evaluated against a particular input
     */
    fun toField(input: LogicalPlan): Field
}