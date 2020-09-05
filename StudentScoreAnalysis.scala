import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lower}

object StudentScoreAnalysis {

  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder()
                                 .appName("StudentScoreAnalysis")
                                 .master("local[2]")
                                 .getOrCreate()

    var studentScoreDf=sparkSession.read
                                   .option("inferSchema","true")
                                   .option("header","true")
                                   .csv("/home/apeksha/MyDataDir/Ex_Files_Big_Data_Hadoop_Apache_Spark/ExerciseFiles/student_scores.csv")

    studentScoreDf.cache()

    //Total score of each student for each subjects
    studentScoreDf=studentScoreDf.withColumn("totalScore",studentScoreDf.col("Class Score")+ studentScoreDf.col("Test Score"))

    //Get the marks of each student in physics
    studentScoreDf.select("student","totalScore").where("subject=='Physics'").show()
    studentScoreDf.filter(lower(studentScoreDf("subject"))==="physics").select("student","totalScore").show()

    //Get Average score of each student across all the subjects
    studentScoreDf.groupBy("student").avg("totalScore").as("avgTotalScore").show()

    //Get the student with highest score in each subject
    val highScoreDf=studentScoreDf.groupBy("subject").max("totalScore")
    studentScoreDf.join(highScoreDf,studentScoreDf.col("totalScore")=== highScoreDf.col("max(totalScore)"))
      .select("student","totalScore").show()
  }
}
