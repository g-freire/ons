// Fonte:http://www.ons.org.br/historico/geracao_energia.aspx
// Formatar dados  para .csv contendo os períodes entre 2017 e 2002 ou o desejado
// Observar em 2002 -  " A crise do apagao"

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val spark = SparkSession.builder().getOrCreate();
val data = spark.read.option("header","true").option("inferSchema","true").csv("geracao-sin-hidraulica-2017-2002.csv");
//formatacao inicial
    println("");println("");
    println("--------+  Geracao de Energia - Hidráulica - GW/h    ");
    println("--------+  SIN   - 2017 - 2002                       ");
    println("--------+  Fonte: ONS                                ");println("");
// data.printSchema()  Check Schema type // data.columns mostra array de colunas
  data.show();println("");
  println("Estatistica Basica ( stddev, mean, count, max, min)");println("");data.describe().show();
  println("Mes de Maio");println("");data.select($"Ano",$"5").show() //Seleciona coluna 5
  println("Mes de Maio acima de 35000 GWh/h");println("");data.filter($"5" > 35000).show(); //Mes Maio com producao acima de 35000 GW/h
  println("Melhores mes de maio");println("");data.filter($"5" > 35000).show(); //Mes Maio com producao acima de 35000 GW/h
//vector assembler for ml
val data = data.select(data("Ano").as("label"),$"1",$"2",$"3",$"4",$"5",$"6",$"7",$"8",$"9",$"10",$"11",$"12")
