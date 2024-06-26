package io.hqew.kquery.logical

import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.Field
import org.apache.arrow.vector.types.pojo.ArrowType
import java.sql.SQLException

class Column(val name: String) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields.find { it.name == name }
            ?: throw SQLException("No column named $name in ${input.schema().fields.map {it.name}}")
    }

    override fun toString(): String {
        return "#$name"
    }
}

fun col(name: String) = Column(name)

fun max(expr: LogicalExpr) = Max(expr)


class ColumnIndex(val i: Int) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields[i]
    }

    override fun toString(): String {
        return "#$i"
    }
}


class LiteralString(val str: String) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(str, ArrowTypes.StringType)
    }

    override fun toString(): String {
        return "'$str'"
    }
}

fun lit(value: String) = LiteralString(value)


class LiteralLong(val n: Long): LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(n.toString(), ArrowTypes.Int64Type)
    }

    override fun toString(): String {
        return n.toString()
    }
}

fun lit(value: Long) = LiteralLong(value)

class LiteralFloat(val n: Float): LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(n.toString(), ArrowTypes.FloatType)
    }

    override fun toString(): String {
        return n.toString()
    }
}

fun lit(value: Float) = LiteralFloat(value)

class LiteralDouble(val n: Double): LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(n.toString(), ArrowTypes.DoubleType)
    }

    override fun toString(): String {
        return n.toString()
    }
}

fun lit(value: Double) = LiteralDouble(value)

class CastExpr(val expr: LogicalExpr, val dataType: ArrowType) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(expr.toField(input).name, dataType)
    }

    override fun toString(): String {
        return "CAST($expr AS $dataType)"
    }
}

fun cast(expr: LogicalExpr, dataType: ArrowType) = CastExpr(expr, dataType)

abstract class BinaryExpr(
    val name: String, val op: String, val l: LogicalExpr, val r: LogicalExpr
) : LogicalExpr {
    override fun toString(): String {
        return "$l $op $r"
    }
}

abstract class UnaryExpr(
    val name: String, val op: String, val expr: LogicalExpr
) : LogicalExpr {
    override fun toString(): String {
        return "$op $expr"
    }
}

/**
 * Representing a logical NOT
 */
class Not(expr: LogicalExpr) : UnaryExpr("not", "NOT", expr) {
    override fun toField(input: LogicalPlan): Field {
        return Field(name, ArrowTypes.BooleanType)
    }
}

abstract class BooleanBinaryExpr(name: String, op: String, l: LogicalExpr, r: LogicalExpr) : BinaryExpr(name, op, l, r) {
    override fun toField(input: LogicalPlan): Field {
        return Field(name, ArrowTypes.BooleanType)
    }
}

/** Logical expression representing a logical AND */
class And(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("and", "AND", l, r)

/** Logical expression representing a logical OR */
class Or(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("or", "OR", l, r)

/** Logical expression representing an equality (`=`) comparison */
class Eq(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("eq", "=", l, r)

/** Logical expression representing an inequality (`!=`) comparison */
class Neq(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("neq", "!=", l, r)

/** Logical expression representing a greater than (`>`) comparison */
class Gt(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("gt", ">", l, r)

/** Logical expression representing a greater than or equals (`>=`) comparison */
class GtEq(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("gteq", ">=", l, r)

/** Logical expression representing a less than (`<`) comparison */
class Lt(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("lt", "<", l, r)

/** Logical expression representing a less than or equals (`<=`) comparison */
class LtEq(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("lteq", "<=", l, r)

infix fun LogicalExpr.eq(rhs: LogicalExpr): LogicalExpr {
    return Eq(this, rhs)
}

infix fun LogicalExpr.neq(rhs: LogicalExpr): LogicalExpr {
    return Neq(this, rhs)
}

infix fun LogicalExpr.gt(rhs: LogicalExpr): LogicalExpr {
    return Gt(this, rhs)
}

infix fun LogicalExpr.gteq(rhs: LogicalExpr): LogicalExpr {
    return GtEq(this, rhs)
}

infix fun LogicalExpr.lt(rhs: LogicalExpr): LogicalExpr {
    return Lt(this, rhs)
}

infix fun LogicalExpr.lteq(rhs: LogicalExpr): LogicalExpr {
    return LtEq(this, rhs)
}

abstract class MathExpr(name: String, op: String, l: LogicalExpr, r: LogicalExpr) : BinaryExpr(name, op, l, r) {

    override fun toField(input: LogicalPlan): Field {
        return Field(name, l.toField(input).dataType)
    }
}

class Add(l: LogicalExpr, r: LogicalExpr): MathExpr("add", "+", l, r)

class Subtract(l: LogicalExpr, r: LogicalExpr): MathExpr("subtract", "-", l, r)

class Multiply(l: LogicalExpr, r: LogicalExpr): MathExpr("mult", "*", l, r)

class Divide(l: LogicalExpr, r: LogicalExpr): MathExpr("div", "/", l, r)

class Modulus(l: LogicalExpr, r: LogicalExpr): MathExpr("mod", "%", l, r)


infix fun LogicalExpr.add(rhs: LogicalExpr): LogicalExpr {
    return Add(this, rhs)
}

infix fun LogicalExpr.subtract(rhs: LogicalExpr): LogicalExpr {
    return Subtract(this, rhs)
}

infix fun LogicalExpr.mult(rhs: LogicalExpr): LogicalExpr {
    return Multiply(this, rhs)
}
infix fun LogicalExpr.div(rhs: LogicalExpr): LogicalExpr {
    return Divide(this, rhs)
}

infix fun LogicalExpr.mod(rhs: LogicalExpr): LogicalExpr {
    return Modulus(this, rhs)
}

class Alias(val expr: LogicalExpr, val alias: String): LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(alias, expr.toField(input).dataType)
    }

    override fun toString(): String {
        return "$expr as $alias"
    }
}

infix fun LogicalExpr.alias(alias: String): Alias {
    return Alias(this, alias)
}

class ScalarFunction(val name: String, val args: List<LogicalExpr>, val returnType: ArrowType) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(name, returnType)
    }

    override fun toString(): String {
        return "$name($args)"
    }
}


abstract class AggregateExpr(val name: String, val expr: LogicalExpr): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field(name, expr.toField(input).dataType)
    }

    override fun toString(): String {
        return "$name($expr)"
    }
}

/** Logical expression representing the SUM aggregate expression. */
class Sum(input: LogicalExpr) : AggregateExpr("SUM", input)

/** Logical expression representing the MIN aggregate expression. */
class Min(input: LogicalExpr) : AggregateExpr("MIN", input)

/** Logical expression representing the MAX aggregate expression. */
class Max(input: LogicalExpr) : AggregateExpr("MAX", input)

/** Logical expression representing the AVG aggregate expression. */
class Avg(input: LogicalExpr) : AggregateExpr("AVG", input)

/** Logical expression representing the COUNT aggregate expression. */
class Count(input: LogicalExpr) : AggregateExpr("COUNT", input) {

    override fun toField(input: LogicalPlan): Field {
        return Field("COUNT", ArrowTypes.UInt32Type)
    }
}

class CountDistinct(input: LogicalExpr) : AggregateExpr("COUNT_DISTINCT", input) {

    override fun toField(input: LogicalPlan): Field {
        return Field("COUNT", ArrowTypes.UInt32Type)
    }

    override fun toString(): String {
        return "COUNT(DISTINCT $expr)"
    }
}