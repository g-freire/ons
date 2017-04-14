// Fonte:http://www.ons.org.br/historico/geracao_energia.aspx
// Formatar dados  para .csv contendo os períodes entre 2017 e 2002 ou o desejado
// Observar em 2002 -  " A crise do apagao"

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val data = spark.read.option("header","true").option("inferSchema","true").csv("ons.csv")
    println("")
    println("")
    println("--------+  Geracao de Energia - Hidráulica - GW/h    ")
    println("--------+  SIN   - 2017 - 2002                       ")
    println("--------+  Fonte: ONS                                ")
    println("")
data.show()
//Filtrando Linha MES Fevereiro
data.filter($"Mês" === "Fev").show()
