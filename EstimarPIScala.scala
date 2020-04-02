package pi

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object EstimadorPi {

  def estimarPi(numero_calculos: Int):Double= {
    var dentro_del_circulo = 0.0
    for (i <- 0 to numero_calculos) {
        val x= math.random()
        val y =  math.random()
        val distancia_al_centro = x * x + y * y
        if (distancia_al_centro <= 1) {
          dentro_del_circulo += 1
        }
    }
    (dentro_del_circulo / numero_calculos) * 4
  }

  def estimarPiBloques(numero_calculos:Int,bloques:Int):Double ={
    var calculos:ListBuffer[Int]=ListBuffer()
    val tamanoBloque:Int = numero_calculos / bloques
    for (i <- 0 until bloques){
      calculos += tamanoBloque
    }
    val estimaciones = calculos.map(estimarPi)
    val pi:Double=estimaciones.sum/ bloques
    // val pi:Double=calculos.map(estimarPi).reduce((a,b) => a+b)/ bloques
    pi
  }
  def estimarPiBloquesSpark(numero_calculos:Int,bloques:Int):Double ={

    val session=SparkSession.builder.master("local[3]").appName("EstimadorPi").getOrCreate()

    var calculos:ListBuffer[Int]=ListBuffer()
    val tamanoBloque:Int = numero_calculos / bloques
    for (i <- 0 until bloques){
      calculos += tamanoBloque
    }

    val pi=session.sparkContext.parallelize(calculos).map(estimarPi).sum / bloques
    session.stop()
    pi
  }

  def main(args:Array[String]):Unit ={
   /* var tin=System.currentTimeMillis()
    var pi=estimarPi(1000*1000*100)
    var tout=System.currentTimeMillis()

    println("PI VALE: "+pi)
    println(" calculado en: "+(tout-tin)+" milisegundos")*/

    val tin=System.currentTimeMillis()
    val pi=estimarPiBloquesSpark(1000*1000*100,3)
    val tout=System.currentTimeMillis()

    println("PI VALE: "+pi)
    println(" calculado en: "+(tout-tin)+" milisegundos")

  }

}
