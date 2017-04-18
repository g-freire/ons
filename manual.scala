/**  SCALA MANUAL ( multi-tab for methods man)

----------- LOGICAL OPERATORS
println((1 == 1) && (2 == 2))
println((1 == 2) || (1 == 2))

----------- FOR LOOPS / IF STATEMENTS
for(num <- Range(1,200)){
  if(num%2 != 0 ){
    println(s"$num impar")
  } else{
    println(s"$num par")
  }
}
----------- DATAFRAMES
df.show().describe()    // descreve estatistica basica
data.select($"0",$"5").show();println(""); //Seleciona coluna 5
data.filter($"5" > 100000).show(); // filtragem acima de 100000

----------- SESSAO SPARK
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.json("stocks.json")   //conferir tipagem

----------- FUNCAO E CALLBACK
val somefunction=(a:type,b:type,c:type)=>a*pow(1+b,c);
val callingfunction = somefunction(a,b,c);

*/
