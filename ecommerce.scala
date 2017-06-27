// importando bibliotecas e carregando dados
import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors


Logger.getLogger("org").setLevel(Level.ERROR) // logger de erros


val spark = SparkSession.builder().getOrCreate()
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("Clean-Ecommerce.csv")
data.printSchema()   // checando tipagem
//  Criando vetores Machine Learning
val df = data.select(data("Yearly Amount Spent").as("label"),$"Avg Session Length",$"Time on App",$"Time on Website",$"Length of Membership")
//vector assembler
val assembler = new VectorAssembler().setInputCols(Array("Avg Session Length","Time on App","Time on Website","Length of Membership")).setOutputCol("features")
// Use the assembler to transform our DataFrame to the two columns: label and features
val output = assembler.transform(df).select($"label",$"features")

// Inicio de modelo sem separacao teste treino
// Create a Linear Regression Model object
// val lr = new LinearRegression()
// // Fit the model to the data and call this model lrModel
// val lrModel = lr.fit(output)
// // Print the coefficients and intercept for linear regression
// println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
// // Summarize the model over the training set and print out some metrics!
// val trainingSummary = lrModel.summary
// // Show the residuals, the RMSE, the MSE, and the R^2 Values.
// trainingSummary.residuals.show()
// println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
// println(s"MSE: ${trainingSummary.meanSquaredError}")
// println(s"r2: ${trainingSummary.r2}")
//
// MAGICA COMECA AQ  ( Model Validation)
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
// Create an array of the training and test data
val Array(training,test) = output.select("label","features").randomSplit(Array(0.7, 0.3), seed = 12345)

val lr = new LinearRegression()

val paramGrid = new ParamGridBuilder().addGrid(lr.regParam,Array(10000,0.01)).build()

val trainValidationSplit = (new TrainValidationSplit()
                            .setEstimator(lr)
                            .setEvaluator(new RegressionEvaluator.setmetricName("r2"))
                            .setEstimatorParamMaps(paramGrid)
                            .setTrainRatio(0.8) )

val model = trainValidationSplit.fit(training)

model.transform(test).select("features", "label", "prediction").show()

//model.validationMetrics
