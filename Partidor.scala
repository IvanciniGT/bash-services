package partidor

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Partidor {

  def main(args: Array[String]): Unit = {

    // Cargo las palabras vacias
    val STOP_WORDS=new ListBuffer[String]()                           // Creo la lista
    val fichero=Source.fromFile("src/main/resources/stopWords.txt")                   // Abro el fichero
    for (line <- fichero.getLines) STOP_WORDS += line                 // Leo las palabras
    fichero.close()                                                   // Cierro conexión con el fichero

    val sesion=SparkSession                     // Abrimos sesión con Spark
      .builder
      .appName("Partidor")                // Le pongo un nombre a la aplicación para verla dsde el UI
      .master("local[2]")                // Abrir un Spark en local con 2 CPUs
                                                 // Nota: Esto va fuera en una applicación real que se pueda ejecutar
                                                 //       en cualquier cluster Spark
      .getOrCreate()                             // Recupero la sesión

    val palabrasEliminadas=sesion.sparkContext.longAccumulator("palabrasEliminadas")  // Crear un acumulador para guardar
                                                                                             // el numero de palabras que filtro

    val STOP_WORDS_ENVIADAS = sesion.sparkContext.broadcast(STOP_WORDS)
    import sesion.implicits._         // Activo funciones implicitas en la sesión para poder transformar a Dataframe los RDDs

    sesion.sparkContext
      /* Leer fichero */              .textFile("src/main/resources/texto.txt")
      /* quitar acentos */            .map(StringUtils.stripAccents)
      /* a minúsculas */              .map( _.toLowerCase() )
      /* separar palabras */          .flatMap(_.split("[^#\\w]+"))
      /* quitar palabras vacias */    .filter(_.length>0)
      /* quitar stop words */         .filter(filtrarPalabras(_,STOP_WORDS_ENVIADAS.value, palabrasEliminadas))
      /* a dataframe */               .toDF()
      /* agrupar por palabra */       .groupBy("value")
      /* contarlas */                 .count()
      /* ordenarlas */                .sort(desc("count"))
      /* solo me quedo con 10 */      .limit(10)
      /* las muestro */               .show()

    println("He borrado: "+palabrasEliminadas+" palabras")
    sesion.stop()  // Cierro sesión con Spark

  }

  def filtrarPalabras(palabra:String,STOP_WORDS:ListBuffer[String],palabrasEliminadas:LongAccumulator):Boolean={
    val seQueda = !STOP_WORDS.contains(palabra)
    if(!seQueda) palabrasEliminadas.add(1)
    seQueda
  }
}
