/**  SCALA MANUAL
<- = IN
MULTI TAB METODOS MANUAL
----------- IF STATEMENTS ---  if () {} else (){}
if (false) {
println(" FALSE")
} else {
  println(" TRUE")
}
 ----------- LOGICAL OPERATORS
  println((1 == 1) && (2 == 2))
  println((1 == 2) || (1 == 2))
  ----------- FOR LOOPS ( par ou impar)
  for(num <- Range(1,200)){
if(num%2 != 0 ){
    println(s"$numero e impar")
  }
  }
  */

// DATAFRAMES
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.json("stocks.json")
df.show()
