import MongoDBHelper._
import com.mongodb.client.model.Projections

import scala.io.Source
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._

object Main {
  def main(args: Array[String]): Unit = {

    // Функция чтения данных из файла / Reading file data function.
    def readFile(filename: String): String = {
      val source = Source.fromFile(filename)
      val fileData = source.mkString
      source.close()
      fileData
    }

    // Подключение к MongoDB. / MongoDB connection.
    val mongoClient: MongoClient = MongoClient("mongodb://localhost:27017")
    val dataBase: MongoDatabase = mongoClient.getDatabase("presidentsDB")
    val collection: MongoCollection[Document] =  dataBase.getCollection("presidents")

    // Загрузка данных в MongoDB. / Loading data into MongoDB.
    val files = (1 to 546) map { i: Int => Document(readFile("E://JsonFiles/" + i + ".json")) }
    collection.insertMany(files).results()

    // Отображение результатов запросов. / Displaying query results.
    println("\nПрезиденты США.")
    collection
      .find(equal("country","United States of America"))
      .projection(Projections.fields(include("fullName", "country"), excludeId()))
      .printResults()

    println("\nПрезиденты, рождённые с 1800 по 1820 годы.")
    collection
      .find(and(gte("birthDate.year",1800), lte("birthDate.year",1820)))
      .projection(Projections.fields(include("fullName", "birthDate"), excludeId()))
      .printResults()

  }
}
