package partidor

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Partidor {
  def main(args: Array[String]): Unit = {

    // Cargo las palabras vacias
    val STOP_WORDS=new ListBuffer[String]()                           // Creo la lista
    val fichero=Source.fromFile("src/main/resources/stopWords.txt")   // Abro el fichero
    for (line <- fichero.getLines) STOP_WORDS += line                 // Leo las palabras
    fichero.close()                                                   // Cierro conexión con el fichero

    val sesion=SparkSession           // Abrimos sesión con Spark
      .builder
      .appName("Partidor")    // Le pongo un nombre a la aplicación para verla dsde el UI
      .master("local[2]")    // Abrir un Spark en local con 2 CPUs
                                      // Nota: Esto va fuera en una applicación real que se pueda ejecutar
                                      //     en cualquier cluster Spark
      .getOrCreate()                  // Recupero la sesión

    import sesion.implicits._         // Activo funciones implicitas en la sesión para poder transformar a Dataframe los RDDs

    sesion.sparkContext
      /* Leer fichero */              .textFile("src/main/resources/texto.txt")
      /* quitar acentos */            .map(StringUtils.stripAccents)
      /* a minúsculas */              .map(_.toLowerCase())
      /* separar palabras */          .flatMap(_.split("[^#\\w]+"))
      /* quitar palabras vacias */    .filter(_.length>0)
      /* quitar stop words */         .filter(!STOP_WORDS.contains(_))
      /* a dataframe */               .toDF()
      /* agrupar por palabra */       .groupBy("value")
      /* contarlas */                 .count()
      /* ordenarlas */                .sort(desc("count"))
      /* solo me quedo con 10 */      .limit(10)
      /* las muestro */               .show()

    sesion.stop()  // Cierro sesión con Spark

  }

}
