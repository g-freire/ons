
// Fonte:http://www.ons.org.br/historico/geracao_energia.aspx
// Formatar dados  para .csv contendo os períodes entre 2017 e 2002 ou o desejado
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.log4j._

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate();
val data = spark.read.option("header","true").option("inferSchema","true").csv("ons.csv"); // conferirindo tipagem
    println("");println("");
    println("--------+  Geracao de Energia - Hidráulica - GW/h    ");
     println("--------+  SIN   - 2017 - 2002                       ");
    println("--------+  Fonte: ONS                                ");println("");
 data.printSchema()
 data.show();println("");
 //   println("Estatistica Basica ( stddev, mean, count, max, min)");println("");data.describe().show();
 //   println("Mes de Maio");println("");data.select($"0",$"5").show() //Seleciona coluna 5
 //   println("Mes de Maio acima de 35000 GWh/h");println("");data.filter($"5" > 35000).show(); //Mes Maio com producao acima de 35000 GW/h
 //   println("Melhores meses de maio");println("");data.filter($"5" > 35000).show(); //Mes Maio com producao acima de 35000 GW/h
 //vector assembler for ml     // ("label","features")
  val df = data.select(data("0").as("label"),$"1",$"2",$"3",$"4",$"5",$"6",$"7",$"8",$"9",$"10",$"11",$"12")
//Criacao de vetor necessario para o treinamento do algoritmo de ML
  val assembler = new VectorAssembler().setInputCols(Array($"1",$"2",$"3",$"4",$"5",$"6",$"7",$"8",$"9",$"10",$"11",$"12")).setOutputCol("features")
//Transformando vetor em duas colunas
  val output = assembler.transform(df).select($"label",$"features")
// Criacao de modelo de Regressao Linear
val lr = new LinearRegression()
// Treinamento do modelo ( deve-se dividir os dados primeiro, por agora iremos treinar todos os dados de uma vez)
val lrModel = lr.fit(output)
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//Sumario e algumas metricas sobre o modelo em comparacao ao set de treinamento
val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
//Mostra os residuos da RL
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"MSE: ${trainingSummary.meanSquaredError}")
println(s"r2: ${trainingSummary.r2}")
