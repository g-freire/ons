
// importando bibliotecas e carregando dados
import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

// logger erros
Logger.getLogger("org").setLevel(Level.ERROR)// logger de erros
// cria sessao spark
val spark = SparkSession.builder().getOrCreate()
// leitura dados e inferencia de tipo dos dados brutos
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("Clean-Ecommerce.csv");data.printSchema()  
//  Criando vetores Machine Learning
val df = data.select(data("Yearly Amount Spent").as("label"),$"Avg Session Length",$"Time on App",$"Time on Website",$"Length of Membership")
//vector assembler
val assembler = new VectorAssembler().setInputCols(Array("Avg Session Length","Time on App","Time on Website","Length of Membership")).setOutputCol("features")
// Use the assembler to transform our DataFrame to the two columns: label and features
val output = assembler.transform(df).select($"label",$"features")

// Inicio de objeto regressao linear 
 val lr = new LinearRegression()
// adiciona os dados ao objeto sem separacao teste treino
 val lrModel = lr.fit(output)
// mostra os coeficientes e intercepcoes da regressao linear
 println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
// resumo do modelo 
 val trainingSummary = lrModel.summary
// mostra os valores de  residuos,de RMSE, de MSE, e de R^2 
 trainingSummary.residuals.show()
 println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
 println(s"MSE: ${trainingSummary.meanSquaredError}")
 println(s"r2: ${trainingSummary.r2}")
// fim da regressao linear do modelo