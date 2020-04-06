package partidor

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ListBuffer
import scala.io.Source

object PartidorStreaming {

  val checkpointDir = "./checkpoints"
  var palabrasEliminadas: LongAccumulator=null
  var STOP_WORDS_ENVIADAS: Broadcast[ListBuffer[String]]=null


  def getPalabrasEliminadas(sesion:SparkContext):LongAccumulator={
    if (palabrasEliminadas==null)
      crearConfiguraciones(sesion)
    palabrasEliminadas
  }

  def getStopWords(sesion:SparkContext):Broadcast[ListBuffer[String]]={
    if (STOP_WORDS_ENVIADAS==null)
      crearConfiguraciones(sesion)
    STOP_WORDS_ENVIADAS
  }

  def crearConfiguraciones(sesion:SparkContext):Unit={
    // Cargo las palabras vacias
    val STOP_WORDS = new ListBuffer[String]() // Creo la lista
    val fichero = Source.fromFile("src/main/resources/stopWords.txt") // Abro el fichero
    for (line <- fichero.getLines) STOP_WORDS += line // Leo las palabras
    fichero.close() // Cierro conexión con el fichero
    palabrasEliminadas = sesion.longAccumulator("palabrasEliminadas") // Crear un acumulador para guardar
    // el numero de palabras que filtro
    STOP_WORDS_ENVIADAS= sesion.broadcast(STOP_WORDS)
  }

  def main(args: Array[String]): Unit = {

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Desactivar el log pesado
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Crea o recupera el streaming
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val streamingSession: StreamingContext = StreamingContext.getOrCreate(checkpointDir, crearNuevoStreamingContext)
    crearConfiguraciones(streamingSession.sparkContext)

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Inicia el streaming
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    streamingSession.start()
    streamingSession.awaitTermination()
  }


  def crearNuevoStreamingContext(): StreamingContext={

    println("CREANDO NUEVO STREAMING SESSION")


    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Configurar sesion SPARK
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val sesion = SparkSession // Abrimos sesión con Spark
      .builder
      .appName("Partidor") // Le pongo un nombre a la aplicación para verla dsde el UI
      .master("local[2]") // Abrir un Spark en local con 2 CPUs
      // Nota: Esto va fuera en una applicación real que se pueda ejecutar
      //       en cualquier cluster Spark
      .getOrCreate() // Recupero la sesión

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Configurar Streaming (LO QUE QUIERO QUE SE HAGA MUCHAS VECES (en mi caso cada 5 seg))
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val streaming = new StreamingContext(sesion.sparkContext, Seconds(5))
    streaming.checkpoint(checkpointDir)
    val palabrasPrevias:RDD[(String,Int)]=streaming.sparkContext.parallelize(List[(String,Int)]())

    val textos: DStream[String] = streaming.socketTextStream("localhost", 10000)

    val palabrasActuales = textos
      /* quitar acentos */ .map(StringUtils.stripAccents)
      /* a minúsculas */ .map(_.toLowerCase())
      /* separar palabras */ .flatMap(_.split("[^#\\w]+"))
      /* quitar palabras vacias */ .filter(_.length > 0)
      /* quitar stop words */ .filter(filtrarPalabras(_, getStopWords(sesion.sparkContext).value,getPalabrasEliminadas(sesion.sparkContext) ))
      .map((_, 1))
      .reduceByKey(_ + _)

    palabrasActuales.foreachRDD (rdd => {
      println("\n\nNueva iteración")
      val palabrasActuales=rdd.sortByKey(ascending=false)
        .map(tupla=>(tupla._2,tupla._1))
        .map(tupla=>(tupla._2,tupla._1))
        .take(10)

      println("Palabras obtenidas en esta iteración (habiendo borrado "+getPalabrasEliminadas(rdd.sparkContext).value+" palabras)")
      palabrasActuales.foreach(println)
    })

    return streaming
  }

  def filtrarPalabras(palabra:String,STOP_WORDS:ListBuffer[String],palabrasEliminadas:LongAccumulator):Boolean={
    val seQueda = !STOP_WORDS.contains(palabra)
    if(!seQueda) palabrasEliminadas.add(1)
    seQueda
  }
}
