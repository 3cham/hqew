import io.hqew.kquery.datasource.ParquetDataSource
import org.junit.Test;
import org.junit.jupiter.api.TestInstance;
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetDataSourceTest {

    @Test
    fun `test ParquetDataSource`() {

        val parquetSource = ParquetDataSource(
            File("../testdata/", "alltypes_plain.parquet").absolutePath
        )

        assertEquals(
            "Schema(fields=[Field(name=id, dataType=Int(32, true)), Field(name=bool_col, dataType=Bool), Field(name=tinyint_col, dataType=Int(32, true)), Field(name=smallint_col, dataType=Int(32, true)), Field(name=int_col, dataType=Int(32, true)), Field(name=bigint_col, dataType=Int(64, true)), Field(name=float_col, dataType=FloatingPoint(SINGLE)), Field(name=double_col, dataType=FloatingPoint(DOUBLE)), Field(name=date_string_col, dataType=Binary), Field(name=string_col, dataType=Binary), Field(name=timestamp_col, dataType=Binary)])",
            parquetSource.schema().toString())

        val data = parquetSource.scan(listOf()).iterator()

        assertEquals(true, data.hasNext())
    }

    @Test
    fun `test ParquetDataSource scan column`() {
        val parquetSource = ParquetDataSource(
            File("../testdata/", "alltypes_plain.parquet").absolutePath
        )

        val idData = parquetSource.scan(listOf("id", "bool_col", "date_string_col", "string_col", "timestamp_col")).iterator()
        assert(idData.hasNext())
        val batch = idData.next()

        assertEquals(batch.fields[0].size(), 8)

        val id = batch.fields[0]
        val values = (0..id.size()).map{ id.getValue(it) ?: "null" }

        assertEquals("4,5,6,7,2,3,0,1,null", values.joinToString(","))

        val bools = batch.fields[1]
        val boolValues = (0..id.size()).map{ bools.getValue(it) ?: "null" }

        assertEquals("true,false,true,false,true,false,true,false,null", boolValues.joinToString(","))

        val dateStr = batch.fields[2]
        val dateStrVal = (0..id.size()).map{ dateStr.getValue(it) ?: "null" }

        assertEquals("03/01/09,03/01/09,04/01/09,04/01/09,02/01/09,02/01/09,01/01/09,01/01/09,null", dateStrVal.joinToString(","))

        val strs = batch.fields[3]
        val strsVal = (0..id.size()).map{ strs.getValue(it) ?: "null" }

        assertEquals("0,1,0,1,0,1,0,1,null", strsVal.joinToString(","))

        val timestamp = batch.fields[4]
        val timestampVal = (0..id.size()).map{ timestamp.getValue(it) ?: "null" }

        assertEquals("0,1,0,1,0,1,0,1,null", timestampVal.joinToString(","))

        assertFalse(idData.hasNext())
    }
}
