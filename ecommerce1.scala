
//aluno: gustavo freire

//importando bibliotecas sincronamente
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

// logger erros
Logger.getLogger("org").setLevel(Level.ERROR) 
// cria sessao spark
val spark = SparkSession.builder().getOrCreate();
// leitura dados e inferencia de tipo dos dados brutos
val data = spark.read.option("header","true").option("inferSchema","true").csv("Clean-Ecommerce.csv");data.show(30);data.printSchema();
// seleciona coluna rotulo e colunas parametros 
val df = data.select(data("Yearly Amount Spent").as("label"),$"Avg Session Length",$"Time on App",$"Time on Website",$"Length of Membership");
// val df2 = df.na.drop().show; ( remover comentário se necessario debuggar tipos nao numericos)
// seleciona matriz de parametros para vetor coluna unica 
val assembler = new VectorAssembler().setInputCols(Array("Avg Session Length","Time on App","Time on Website","Length of Membership")).setOutputCol("features")
// seleciona coluna rotulo e coluna parametros para dataframe final
val output = assembler.transform(df).select($"label",$"features")
//fim tratamento dos dados brutos

// Validacao e escolha do melhor modelo  -----------------------

// separacao do modelo em treino e teste
val Array(training, test) = output.select("label","features").randomSplit(Array(0.7, 0.3))
// cria objeto de regressao linear
val lr = new LinearRegression();		
// criacao de grid de parametros a serem combinados
val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.0001,0.001,0.01,0.1, 0 ,1.0,10,100,1000,100000,100000)).addGrid(lr.elasticNetParam, Array(0.1, 0.5, 1)).build()
//criacao do obj de treino e validacao
val trainValidationSplit = (new TrainValidationSplit()
                            .setEstimator(lr)
                            .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                            .setEstimatorParamMaps(paramGrid)
                            .setTrainRatio(0.9) )
// treinamento dos 70% dos dados no obj de treino e validacao
val model = trainValidationSplit.fit(training)
// mostra metricas das validacao
model.validationMetrics
// mostra melhor modelo
model.bestModel
// comparacao com os dados de teste
model.transform(test).select("features", "label", "prediction").show()
// fim da validacao do modelo

//  Mostra os valores de residuos, o RMSE, o MSE e o R^2 do modelo escolhido
 val trainingSummary = model
 trainingSummary.residuals.show()
 println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
 println(s"MSE: ${trainingSummary.meanSquaredError}")
 println(s"r2: ${trainingSummary.r2}")

