import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.functions._
object AvgOfNumericColumns {

  def main(args: Array[String]): Unit = {

    val newSchema = StructType(Array(
      StructField("col1", IntegerType),
      StructField("col2", IntegerType),
      StructField("col3", IntegerType),
      StructField("col4", IntegerType),
      StructField("col5", IntegerType)
    ))
    val encoder=RowEncoder.apply(newSchema)

    val spark = SparkSession.builder()
      .appName("AvgOfNumericColumns")
      .master("local[2]")
      .getOrCreate()

    val df=spark.read
      .option("header","false")
      .csv("/home/apeksha/Workspace/src/main/resources/*")

    val numericDf=df.map(row => iterateRow(row),encoder)
    val meanColDf=numericDf.select(numericDf.columns.map(mean(_)): _*)

    meanColDf.foreach(row => {
      for(index <- 0 to (row.length-2)){
        println("Avg("+index+","+(index+1)+"): " + ((row.getDouble(index)+row.getDouble(index+1))/2))
      }
    })
  }

  def iterateRow(row: Row) : Row={
    var count=0
    var arr = new Array[Int](row.schema.fields.length)

    for( index <- 0 to (row.schema.fields.length-1)){
      if(!row.getString(index).matches("[a-zA-Z]")){
        arr(count) = row.getString(index).toInt
        count = count+1
      }
    }
    //RowFactory.create(arr.toSeq)
    Row.fromSeq(arr.toSeq)
  }
}
