// Databricks notebook source
import com.networknt.schema.{JsonSchema, JsonSchemaFactory, SpecVersion}
import com.fasterxml.jackson
import java.net.{URI, URL}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.File
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.col
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import com.networknt.schema.ValidationMessage
import scala.collection.mutable.HashMap
import com.fasterxml.jackson.databind.node.ObjectNode

// COMMAND ----------

object NodeFields {
  val actor = "actor"
  val provider = "provider"
  val originUrl = "origin.url"
  val objectEvent = "object"
  val typeEvent = "@type"
  val id = "@id"
  val category = "category"
  val tags = "tags"
  val origin = "origin"
  val url = "url"
  val schema = "schema"
  val device = "device"
  val secondaryId = "id"
  val envId = "environmentId"
  val position = "position"
  val jweIds = "jweIds"
  val location = "location"
  val scrollPosition = "scrollPosition"
  val duration = "duration"
  val remoteAddress = "spt:remoteAddress"
  val remoteAddressV6 = "spt:remoteAddressV6"
  val additionalIds = "additionalIds"
  val userId = "spt:userId"
  val environmentId = "environmentId"
  val additionalDeviceIds = "additionalDeviceIds"
}

// COMMAND ----------

object Validation {
  private val factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4)
  private val mapper = new ObjectMapper()

  def getJsonTree(content: String): JsonNode = mapper.readTree(content)

  def getJsonSchemaFromStringContent(schemaContent: String): JsonSchema = {
    factory.getSchema(schemaContent)
  }

  def getJsonSchemaFromUrl(uri: String): JsonSchema = {
    factory.getSchema(new URI(uri))
  }
}

// COMMAND ----------

object Utils {

  def getItemsInListNode(node: JsonNode): Int = {
    node.toString().split(",").toList.length - 1
  }

  def getProvider(eventsData: String): String = {
    val providerField = Validation.getJsonTree(eventsData)
      .get(NodeFields.provider)
      .get(NodeFields.id)
      .asText()
    getClientId(providerField)
  }

  def getClientId(field: String): String = {
    // sdrn:schibsted:client:brand -> brand
    field.split(":")(3)
  }

  val getClientIdUdf = udf[String, String](getClientId)

}

// COMMAND ----------

// MAGIC %md
// MAGIC Read Pulse Data from pulse-box 

// COMMAND ----------

object DataRetriever {
  val providers = List("/client=brandA/*",
    "/client=brandB/*",
    "/client=brandC/*",
    "/client=brandD/*",
    "/client=brandE/*")
  val readPathYellowData = // insert treadPathYellowData
  val readPathRedData = // insert treadPathRedData
  val tmpWritePathRed = // insert ttmpWritePathRed
  val tmpWritePathYellow = // insert tmpWritePathYellow


  //get generation from S3 path
  def getGen(field: String): Int = {
    //pathToAwsBucket/*/gen=0/client=brandA/* -> 0/client=brandB/* -> 0
    field.split("gen=")(1)(0).asDigit
  }

  def getMaxGen(day: String, readPath: String): String = {
    dbutils.fs.ls(readPath + day + "/hour=0/").toList.map(path => getGen(path.toString)).max.toString
  }

  def getPaths(startDate: Int, endDate: Int, readPath: String): List[String] = {
    val dates = startDate to endDate
    val pathWithGen = dates.map(date => readPath + date + "/*/gen=" + getMaxGen(date.toString, readPath).toString)
    pathWithGen.map(path => providers.map(provider => path + provider)).flatten.toList
  }


  def writeToPath(path: String, data: DataFrame): Unit = {
    data
      .withColumn("brand", Utils.getClientIdUdf(col("provider.@id")))
      .write.mode(SaveMode.Overwrite).partitionBy("brand")
      .json(path)
  }

  def readFromPath(path: String): DataFrame = {
    sc.wholeTextFiles(path + "*.json").toDF()
  }

}

// COMMAND ----------

val sampleFraction = 0.00005
val pathsRed = DataRetriever.getPaths(27, 30, DataRetriever.readPathRedData)
val pulseRed = spark.read.parquet(pathsRed: _*).sample(sampleFraction)
val pathsYellow = DataRetriever.getPaths(27, 30, DataRetriever.readPathYellowData)
val pulseYellow = spark.read.parquet(pathsYellow: _*).sample(sampleFraction)


// COMMAND ----------

// MAGIC %md
// MAGIC Write Pulse Data to user-areas bucket in json 

// COMMAND ----------

DataRetriever.writeToPath(DataWriter.tmpWritePathRed, pulseRed)
DataRetriever.writeToPath(DataWriter.tmpWritePathYellow, pulseYellow)

// COMMAND ----------

// MAGIC %md
// MAGIC Read Pulse JSON from user-area bucket

// COMMAND ----------

val jsonRed = DataRetriever.readFromPath(DataRetriever.tmpWritePathRed + "*.json")
val jsonYellow = DataRetriever.readFromPath(DataRetriever.tmpWritePathYellow + "*.json")

// COMMAND ----------

jsonRed.show(1)

// COMMAND ----------

jsonYellow.show(1)

// COMMAND ----------

// MAGIC %md
// MAGIC CAM CHECK

// COMMAND ----------

object CAMCheck {

  def getCamPlan: List[String] = {
    val camTrackingPlan = scala.io.Source.fromURL("https://6xje2u5uhe.execute-api.eu-north-1.amazonaws.com/pro/cam").mkString
    //Schemas is type of ARRAY and it stores the CAM fields as list inside "schema" ex. "schemas":[{"schema":[
    val camNode = Validation.getJsonTree(camTrackingPlan).get("schemas").get(0).get("schema")
    val itemCount = 0 to Utils.getItemsInListNode(camNode) //used to access CAMFields in "schema" which is also of type ARRAY
    itemCount.map(index => camNode.get(index).asText).toList
  }

  val requiredCAMFieldsList = getCamPlan

  def checkCAMField(node: JsonNode, fields: String): Int = {
    if (fields.contains(NodeFields.actor) && skipActor(node)) 0
    else if (fields.contains(NodeFields.originUrl)) urlExists(node)
    else {
      //the fields are in this format [".tracker.type",  ".tracker.version" ], so remove the "."
      val fieldSplitted = fields.split("\\.")
      fieldSplitted.length match {
        //if the field is .tracker.type then it is split in the following way: [".", "tracker", "type"] 
        case 3 => CAMFieldExists(node.get(fieldSplitted(1)), fieldSplitted(2))
        //if the field is .provider then it is split in the following way: [".", "provider"]
        case 2 => CAMFieldExists(node, fieldSplitted(1))
      }
    }
  }

  def checkCAM(node: JsonNode): Int = {
    requiredCAMFieldsList.map(requiredCAMFields => checkCAMField(node, requiredCAMFields)).reduceLeft(_ max _)
  }

  //if the event does not contain actor, do not check the CAM field for .actor
  def skipActor(node: JsonNode): Boolean = {
    try {
      node.get(NodeFields.actor) == null
    } catch {
      case e: Exception => true
    }
  }

  //the url can be === when visiting the site for the first time so only check if it exists
  def urlExists(node: JsonNode): Int = {
    try {
      if (node.get(NodeFields.origin).get(NodeFields.url) == null) 1 else 0
    } catch {
      case e: Exception => 1
    }
  }

  def CAMFieldExists(node: JsonNode, field: String): Int = {
    try {
      node.get(field).toString().length()
      0
    } catch {
      case e: Exception => 1
    }
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ATE Check

// COMMAND ----------

object ATECheck {

  val eventType = "View"
  val objectType = List("Article", "ClassifiedAd", "Listing")
  val stringType = "STRING"
  val na = "NA"


  def checkEventType(node: JsonNode): String = {
    try {
      val nodeEventType = node.get(NodeFields.typeEvent).asText
      val objectNodeType = node.get(NodeFields.objectEvent).get(NodeFields.typeEvent).asText
      if (nodeEventType == eventType) {
        if (objectNodeType == objectType(1)) "onlyCategory"
        else if (objectNodeType == objectType(0)) "categoryAndTags"
        else if (objectNodeType == objectType(2)) "query"
        else na
      } else {
        na
      }
    } catch {
      case e: Exception => na
    }
  }


  //returns (errors for Categories, errors for tags, error for categories + tags, ATE errors for queries, total amount of events checked against ATE checks)
  def ATECheck(jsonTree: JsonNode): (Int, Int, Int, Int) = {
    checkEventType(jsonTree) match {
      //validate only category when @type: View and object.@type: ClassifiedAd  
      case "onlyCategory" => {
        val errorCat = validateCategory(jsonTree)
        (errorCat, 0, 0, 1)
      }
      //validate category and tags when @type: View and object.@type: Article
      case "categoryAndTags" => {
        val errorCat = validateCategory(jsonTree)
        val errorTag = validateTags(jsonTree)
        (errorCat, errorTag, 0, 1)
      }
      //validate category and tags when @type: View and object.@type: Listing
      case "query" => {
        val errorQuery = validateQuery(jsonTree)
        (0, 0, errorQuery, 1)
      }
      case _ => (0, 0, 0, 0)
    }
  }


  //checks if category  is String type and format it so no " are included and if it is a list of categories such as "A > B, C > D" split by ", "
  def validateCategory(node: JsonNode): Int = {
    try {
      val categories = node.get(NodeFields.objectEvent).get(NodeFields.category)
      if (categories.getNodeType().toString() != stringType) 1
      //if multiple categories "A > B, C > D" split at ", "
      val categoriesToList = categories.asText.split(", ").toList
      categoriesToList.map(checkCategoryPattern(_)).max
    } catch {
      case e: Exception => 1 //if event does not contain category, return 1
    }
  }


  def checkCategoryPattern(category: String): Int = {
    // "A > B > A" -> List(A, B, A)
    val categoryList = category.replaceAll("\\s", "").split(">").toList
    //if category contains doubles category names or contains commas return 1
    if (categoryList.distinct.length != categoryList.length || category.contains(",")) {
      1
    } else {
      // regex allows words starting with Capital letters, white space before and after > and characters besides letters and "-" fails the regex 
      // no numbers allowed
      val categoryPattern: Regex = """([A-ZÆØÄÖÅ]+[a-zæøäöå-]*)+(\s[a-zA-ZæøäöåÆØÄÖÅ-]*)*(\s>\s[A-ZÆØÄÖÅ-]+[a-zæøäöå-]*(\s[a-zA-ZæøäöåÆØÄÖÅ-]*)*)*""".r
      category match {
        case categoryPattern(_*) => 0
        case _ => 1
      }
    }
  }

  //checks if tags is of ARRAY type and do not contain "www"
  def validateTags(node: JsonNode): Int = {
    try {
      val tags = node.get(NodeFields.objectEvent).get(NodeFields.tags)
      tags.getNodeType().toString() match {
        case "ARRAY" => (if (tags.toString().contains("www")) 1 else 0) | isTagStringType(tags, ",")
        case _ => 1
      }
    } catch {
      case e: Exception => 1 //if event does not contain tags, return 1
    }
  }

  //checks if one tag is a STRING
  def isTagStringType(node: JsonNode, delimiter: String): Int = {
    //tags are stored in an array, tagsCount is used as index to access them in the node
    val tagsCount = 0 to Utils.getItemsInListNode(node)
    tagsCount.map(index => if (node.get(index).getNodeType().toString == stringType) 0 else 1).max
  }


  //checks if query is type of STRING
  def validateQuery(node: JsonNode): Int = {
    try {
      if (node.get(NodeFields.objectEvent).get("filters").get("query").getNodeType().toString() == stringType) 0 else 1
    } catch {
      case e: Exception => 1 //if event does not contain object.filters.query, return 1
    }
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Schema Check

// COMMAND ----------

object SchemaCheck {
  var schemaHashMap: HashMap[String, String] = new HashMap()
  var errorMap: HashMap[String, Int] = new HashMap()
  val defaultInteger: Int = 1
  val defaultNullId: JsonNode = Validation.getJsonTree("""{"nullId": "00000000-0000-4000-8000-000000000000"}""")
  val nullNode: JsonNode = Validation.getJsonTree("""{"id": null}""")
  val defaultActorId = "sdrn:schibsted.com:user:7103400"
  val defaultId = "41248324-0ca1-40bc-963a-c264d28054c7"
  val defaultAddress = "0.0.0.0"
  val defaultEnvironmentId = "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708"
  val nullId = "nullId"
  val defaultNumbers = ":5a54743e-e199-4f52-9ebf-07535f04b708"
  val illegalEventType = List("Touchpoint", "Subscription", "Purchase", "Cancel")
  val sptCustom = "spt:custom"

  //The following transformations are applied to the Data since all personal information are not present in the data which 
  //will fail the schema. Some default values are put in those fields. 
  //.actor 
  def transform(node: JsonNode): JsonNode = {
    if (node.get(NodeFields.actor) != null) {
      //set actor.@id to "sdrn:schibsted.com:user:7103400"
      node.get(NodeFields.actor).asInstanceOf[ObjectNode].put(NodeFields.id, defaultActorId)
      //set actor.spt:remoteAddress to "0.0.0.0"
      if (node.get(NodeFields.actor).get("actorRemoteAddress") == null) {
        node.get(NodeFields.actor).asInstanceOf[ObjectNode].put(NodeFields.remoteAddress, defaultAddress)
      }
      //set actor.spt:remoteAddressV6 to empty string
      if (node.get(NodeFields.actor).get(NodeFields.remoteAddressV6) == null) {
        node.get(NodeFields.actor).asInstanceOf[ObjectNode].put(NodeFields.remoteAddressV6, "")
      }
      //set actor.spt:userId to "sdrn:schibsted.com:user:7103400"
      if (node.get(NodeFields.actor).get(NodeFields.userId) == defaultNullId.get(nullId)) {
        node.get(NodeFields.actor).asInstanceOf[ObjectNode].put(NodeFields.userId, defaultActorId)
      }
      //remove actor.additionalIds.@id 
      if (node.get(NodeFields.actor).get(NodeFields.additionalIds) == null) {
        node.get(NodeFields.actor).asInstanceOf[ObjectNode].remove(NodeFields.additionalIds)
      }
    }
    //remove location if null
    if (node.get(NodeFields.location) == nullNode.get(NodeFields.secondaryId)) {
      node.asInstanceOf[ObjectNode].remove(NodeFields.location)
    }
    //set @id to "41248324-0ca1-40bc-963a-c264d28054c7"
    if (node.get(NodeFields.id) != null) {
      node.asInstanceOf[ObjectNode].put(NodeFields.id, defaultId)
    }
    //set device.enviornmentId to "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708"
    if (node.get(NodeFields.device).get(NodeFields.environmentId) == defaultNullId.get(nullId)) {
      node.get(NodeFields.device).asInstanceOf[ObjectNode].put(NodeFields.environmentId, defaultEnvironmentId)
    }
    //remove device.additionalDeviceIdS if null
    if (node.get(NodeFields.device).get(NodeFields.additionalDeviceIds) == nullNode.get(NodeFields.secondaryId)) {
      node.get(NodeFields.device).asInstanceOf[ObjectNode].remove(NodeFields.additionalDeviceIds)
    }
    //set object.@id to "sdrn:schibsted:" + eventType + ":5a54743e-e199-4f52-9ebf-07535f04b708" if object.@id is null or contains "sdrn"
    if (node.get(NodeFields.objectEvent).get(NodeFields.id) == null || !node.get(NodeFields.objectEvent).get(NodeFields.id).toString().contains("sdrn")) {
      val objectId = "sdrn:schibsted:" + node.get(NodeFields.objectEvent).get("type").toString.toLowerCase() + defaultNumbers
      node.get(NodeFields.objectEvent).asInstanceOf[ObjectNode].put(NodeFields.id, objectId)
    }
    //remove device.jewIds
    if (node.get(NodeFields.device).get(NodeFields.jweIds) != null) {
      node.get(NodeFields.objectEvent).asInstanceOf[ObjectNode].remove(NodeFields.jweIds)
    }
    //remove object.spt:custom
    if (node.get(NodeFields.objectEvent).get(sptCustom) != null) {
      node.get(NodeFields.objectEvent).asInstanceOf[ObjectNode].remove(sptCustom)
    }
    //The following fields are changed from floats into int since it is possible the type of the field is changed during reading and writing operations to/from S3
    //position
    if (node.get(NodeFields.position) != null) {
      node.asInstanceOf[ObjectNode].put(NodeFields.position, defaultInteger)
    }
    //object.position
    if (node.get(NodeFields.device).get(NodeFields.position) != null) {
      node.get(NodeFields.device).asInstanceOf[ObjectNode].remove(NodeFields.position)
    }
    //duration
    if (node.get(NodeFields.duration) != null) {
      node.asInstanceOf[ObjectNode].put(NodeFields.duration, defaultInteger)
    }
    //scrollposition
    if (node.get(NodeFields.scrollPosition) != null) {
      node.asInstanceOf[ObjectNode].put(NodeFields.scrollPosition, defaultInteger)
    }
    node
  }

  def checkSchema(node: JsonNode): Int = {
    try {
      //Do not run schema checks on events of Touchpoint, Subscription, Purchase and Cancel type 
      if (illegalEventType.contains(node.get(NodeFields.objectEvent).get(NodeFields.typeEvent))) return 0

      val nodeTransformed = transform(node)
      val schemaUri = nodeTransformed.get(NodeFields.schema).asText
      val schema = Validation.getJsonSchemaFromStringContent(getSchemaFromHashMap(schemaUri))
      val errors = schema.validate(nodeTransformed)
      val JweIdsExist = isJweIdsNull(nodeTransformed)
      val errorListFiltered = filterErrors(errors.asScala, JweIdsExist)
      errorListFiltered.size compare 0 match {
        case 0 => 0 //if no errors return 0
        case 1 => 1 //if there are errors return 1
      }
    } catch {
      case e: Exception => 1
    }
  }

  def isJweIdsNull(node: JsonNode): Boolean = {
    try {
      node.get(NodeFields.device).get(NodeFields.jweIds).asText == "null"
    } catch {
      case e: Exception => false
    }
  }


  def filterErrors(errors: scala.collection.mutable.Set[com.networknt.schema.ValidationMessage], JweIdsExist: Boolean): scala.collection.mutable.Set[com.networknt.schema.ValidationMessage] = {
    JweIdsExist match {
      case true => errors.filter(x =>
        !(x.toString().contains(sptCustom) ||
          x.toString().contains("$.device.jweIds")))
      case _ => errors.filter(x =>
        !(x.toString().contains(sptCustom))) 
    }
  }

  //this function can be used for testing purposes
  //it stores the error and the amount of occurences (key: error, vale: occurences)
  def addErrortoHashMap(error: String): Unit = {
    try {
      errorMap.update(error, errorMap(error) + 1) //add error to hashMap and increment by 1 occurences 
    } catch {
      case e: Exception => {
        errorMap += (error -> 1)
      } //if error not found in hashmap, add it 
    }
  }


  def getSchemaFromHashMap(url: String): String = {
    try {
      schemaHashMap(url)
    } catch {
      case e: Exception => {
        //when reading schema from url thr response is ""#" : {"additionalProperties":true, ..." apply .substring(5) to get the right schema format
        schemaHashMap += (url -> Validation.getJsonSchemaFromUrl(url).toString.substring(5))
        getSchemaFromHashMap(url)
      }
    }
  }


}

// COMMAND ----------

def runChecksOnEvent(event: String): (Int, Int, Int, Int, Int, Int) = {
  val jsonTree = Validation.getJsonTree(event)
  val schemaErrors = SchemaCheck.checkSchema(jsonTree)
  val camErrors = CAMCheck.checkCAM(jsonTree)
  val ATEErrors = ATECheck.ATECheck(jsonTree)
  (schemaErrors, camErrors, ATEErrors._1, ATEErrors._2, ATEErrors._3, ATEErrors._4)
}


def runChecksOnFile(events: String): (Int, Int, Int, Int, Int, Int) = {
  val eventsList = events.split("\n")
  eventsList.map(runChecksOnEvent(_)).reduce((a, b) => (a._1 + b._1,
    a._2 + b._2,
    a._3 + b._3,
    a._4 + b._4,
    a._5 + b._5,
    a._6 + b._6))
}

def eventsTotalCount(events: String): Int = {
  events.mkString.split("\n").length

}

def getProvider(events: String): String = {
  val eventsList = events.mkString.split("\n")
  Utils.getProvider(eventsList(0)).toString //all events in a file have the same provider, enough just to check the first event's provider
}

val runChecksDf = udf[(Int, Int, Int, Int, Int, Int), String](runChecksOnFile)
val getProviderdf = udf[String, String](getProvider)
val eventsTotalCountdf = udf[Int, String](eventsTotalCount)

def runAllDQChecks(events: DataFrame): DataFrame = {
  events.withColumn("DQ errors", runChecksDf(col("_2"))) //column returned as [Schema errors, CAM errors, ATE errors]
    .withColumn("Total Events", eventsTotalCountdf(col("_2")))
    .withColumn("provider", getProviderdf(col("_2")))
    .select($"DQ errors", $"DQ errors._1".as("schema error"),
      $"DQ errors._2".as("CAM error"),
      $"DQ errors._3".as("Category error"),
      $"DQ errors._4".as("Tags error"),
      $"DQ errors._5".as("Query error"),
      $"DQ errors._6".as("Total ATE error"),
      $"provider", $"Total Events")
    .drop("DQ errors")
    .groupBy("provider")
    .sum()
}


// COMMAND ----------

// MAGIC %md
// MAGIC Run DQ Checks on JSON Pulse Data

// COMMAND ----------

val errorsRed = runAllDQChecks(jsonRed.limit(10))
val errorsYellow = runAllDQChecks(jsonYellow.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### TEST Data

// COMMAND ----------

//First event: All CAM fields but no actor and url field not preset--> should fail
//Second event: ALL CAM fields but no device.enviornment and .origin.url === "" --> should fail
//Second event: ALL CAM fields but actor is missing all fields--> should fail
//Second event: ALL CAM fields are missing --> should fail


val eventsIncorrectCAM = List("""
    {"schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "spt:pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "duration": 10000,
    "scrollPosition": 0,
    "@id": "61ca5ba0-d4dc-451b-aedc-70e6ae718179",
    "@type": "Engagement",
    "pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "creationDate": "2022-09-25T07:56:28.042Z",
    "experiments": [
        {
            "@id": "sdrn: curate:experiment: ab - conversion - ranker - imp - warmup - 3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
         "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "@type": "Device",
        "acceptLanguage": "sv - SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla / 5.0(Macintosh; Intel Mac OS X 10_15_7) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 105.0.0.0 Safari / 537.36",
        "deviceType": "desktop",
        "viewportSize": "1103x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn: aftonbladet:frontpage: 026fdb5a - 626d - 469e-90b1 - 9d7801442477",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:pageId": "026fdb5a - 626d - 469e-90b1 - 9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 40
        },
        "name": "Nyheter från Sveriges största nyhetssajt",
        "url": "https://www.aftonbladet.se/"
    },
    "origin": {
        "url2": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "product": "hyperionX",
        "productType": "ResponsiveWeb",
        "productTag": "Aftonbladet",
        "spt:engage": "AB"
    },
    "session": {
        "@id": "sdrn:schibsted:session:faeae7a8-ba10-4fec-b786-33a6963964eb",
        "startTime": "2022-09-25T07:56:18.019Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    }
}""",










"""{"experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/"
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",







"""{
    "actor": {
        "@idF": "00000000-0000-4000-8000-000000000000",
        "spt:userIdF": "00000000-0000-4000-8000-000000000000",
        "subscriptionNameF": "No"
    },
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null",
        "additionalDeviceIds": null
    },
    "object": {
        "@id": "sdrn:aftonbladet:mediaad:382258706",
        "@type": "MediaAd",
        "name": "Aftonbladet Direkt - Skidorter oroade över höga elkostnader",
        "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075",
        "assetType": "Preroll",
        "muted": true,
        "fullscreen": false,
        "autoplay": true,
        "mediaAssetId": "sdrn:aftonbladet:mediaasset:347346",
        "adSequenceCount": 1,
        "apiUrl": "https://svp.vg.no/svp/api/v1/ab/assets/347346?appName=linkpulse",
        "adSequenceDuration": [
            20
        ],
        "adSequencePosition": 1,
        "duration": 20,
        "category": "play > webbtv > nyheter",
        "sticky": true,
        "page": {
            "@id": "sdrn:aftonbladet:article:Rr77qd",
            "@type": "Article",
            "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075"
        }
    },
    "origin": {
        "url": "https://www.aftonbladet.se/callback?state=9c342c19ab7ea"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "spt:engage": "AB",
        "product": "hyperionX",
        "productTag": "Aftonbladet",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "intent": "Watch",
    "@type": "Engagement",
    "position": 12.0,
    "duration": 12000.0,
    "@id": "12344",
    "pageViewId": "7eb2c73a-8e49-4b3c-a5b2-1410dad44a99",
    "creationDate": "2022-09-25T18:12:04.366Z"
}""",







"""{
    "actor": {
        "subscriptionName": "No"
    },
    "experiments": [
        {
            "@id": "sdrn:curate:experiment:ab-conversion-ranker-imp-warmup-3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
        "acceptLanguage": "sv-SE",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "spt:custom": {
            "spt:pageId": "026fdb5a-626d-469e-90b1-9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 33
        }
    },
    "provider": {
        "@type": "Organization",
        "spt:engage": "AB"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "eventBuilderVersion": "1.2.5"
    },
    "spt:pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
    "duration": 5000,
    "scrollPosition": 0,
    "pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
    "location": null
}
"""
)

// COMMAND ----------

//First event: All CAM fields but no actor --> should pass
//Second event: ALL CAM fields but no actor and .origin.url === "" --> should pass
//Second event: ALL CAM fields --> should pass
//Second event: ALL CAM fields but .origin.url === "" --> should pass


val eventsCorrectCAM = List("""
    {"schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "spt:pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "duration": 10000,
    "scrollPosition": 0,
    "@id": "61ca5ba0-d4dc-451b-aedc-70e6ae718179",
    "@type": "Engagement",
    "pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "creationDate": "2022-09-25T07:56:28.042Z",
    "experiments": [
        {
            "@id": "sdrn: curate:experiment: ab - conversion - ranker - imp - warmup - 3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv - SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla / 5.0(Macintosh; Intel Mac OS X 10_15_7) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 105.0.0.0 Safari / 537.36",
        "deviceType": "desktop",
        "viewportSize": "1103x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn: aftonbladet:frontpage: 026fdb5a - 626d - 469e-90b1 - 9d7801442477",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:pageId": "026fdb5a - 626d - 469e-90b1 - 9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 40
        },
        "name": "Nyheter från Sveriges största nyhetssajt",
        "url": "https://www.aftonbladet.se/"
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "product": "hyperionX",
        "productType": "ResponsiveWeb",
        "productTag": "Aftonbladet",
        "spt:engage": "AB"
    },
    "session": {
        "@id": "sdrn:schibsted:session:faeae7a8-ba10-4fec-b786-33a6963964eb",
        "startTime": "2022-09-25T07:56:18.019Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    }
}""",










"""{"experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/"
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",







"""{
    "actor": {
        "@id": "00000000-0000-4000-8000-000000000000",
        "spt:userId": "00000000-0000-4000-8000-000000000000",
        "subscriptionName": "No"
    },
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null",
        "additionalDeviceIds": null
    },
    "object": {
        "@id": "sdrn:aftonbladet:mediaad:382258706",
        "@type": "MediaAd",
        "name": "Aftonbladet Direkt - Skidorter oroade över höga elkostnader",
        "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075",
        "assetType": "Preroll",
        "muted": true,
        "fullscreen": false,
        "autoplay": true,
        "mediaAssetId": "sdrn:aftonbladet:mediaasset:347346",
        "adSequenceCount": 1,
        "apiUrl": "https://svp.vg.no/svp/api/v1/ab/assets/347346?appName=linkpulse",
        "adSequenceDuration": [
            20
        ],
        "adSequencePosition": 1,
        "duration": 20,
        "category": "play > webbtv > nyheter",
        "sticky": true,
        "page": {
            "@id": "sdrn:aftonbladet:article:Rr77qd",
            "@type": "Article",
            "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075"
        }
    },
    "origin": {
        "url": "https://www.aftonbladet.se/callback?state=9c342c19ab7ea"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "spt:engage": "AB",
        "product": "hyperionX",
        "productTag": "Aftonbladet",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "intent": "Watch",
    "@type": "Engagement",
    "position": 12.0,
    "duration": 12000.0,
    "@id": "12344",
    "pageViewId": "7eb2c73a-8e49-4b3c-a5b2-1410dad44a99",
    "creationDate": "2022-09-25T18:12:04.366Z"
}""",







"""{
    "actor": {
        "@id": "s00000000-0000-4000-8000-000000000000",
        "spt:userId": "00000000-0000-4000-8000-000000000000",
        "subscriptionName": "No"
    },
    "experiments": [
        {
            "@id": "sdrn:curate:experiment:ab-conversion-ranker-imp-warmup-3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "1138x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftonbladet:frontpage:026fdb5a-626d-469e-90b1-9d7801442477",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:pageId": "026fdb5a-626d-469e-90b1-9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 33
        },
        "name": "Nyheter från Sveriges största nyhetssajt",
        "url": "https://www.aftonbladet.se/"
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "product": "hyperionX",
        "productType": "ResponsiveWeb",
        "productTag": "Aftonbladet",
        "spt:engage": "AB"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "spt:pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
    "duration": 5000,
    "scrollPosition": 0,
    "@id": "1234",
    "@type": "Engagement",
    "pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
    "creationDate": "2022-09-25T18:39:37.597Z",
    "location": null
}
"""
)

// COMMAND ----------

val eventsSchemaCorrect = List("""
    {"schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "spt:pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "duration": 10000,
    "scrollPosition": 0,
    "@id": "61ca5ba0-d4dc-451b-aedc-70e6ae718179",
    "@type": "Engagement",
    "pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "creationDate": "2022-09-25T07:56:28.042Z",
    "experiments": [
        {
            "@id": "sdrn: curate:experiment: ab - conversion - ranker - imp - warmup - 3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv - SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla / 5.0(Macintosh; Intel Mac OS X 10_15_7) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 105.0.0.0 Safari / 537.36",
        "deviceType": "desktop",
        "viewportSize": "1103x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn: aftonbladet:frontpage: 026fdb5a - 626d - 469e-90b1 - 9d7801442477",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:pageId": "026fdb5a - 626d - 469e-90b1 - 9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 40
        },
        "name": "Nyheter från Sveriges största nyhetssajt",
        "url": "https://www.aftonbladet.se/"
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "product": "hyperionX",
        "productType": "ResponsiveWeb",
        "productTag": "Aftonbladet",
        "spt:engage": "AB"
    },
    "session": {
        "@id": "sdrn:schibsted:session:faeae7a8-ba10-4fec-b786-33a6963964eb",
        "startTime": "2022-09-25T07:56:18.019Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    }
}""",

"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",
"""{
    "actor": {
        "@id": "00000000-0000-4000-8000-000000000000",
        "spt:userId": "00000000-0000-4000-8000-000000000000",
        "subscriptionName": "No"
    },
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null",
        "additionalDeviceIds": null
    },
    "object": {
        "@id": "sdrn:aftonbladet:mediaad:382258706",
        "@type": "MediaAd",
        "name": "Aftonbladet Direkt - Skidorter oroade över höga elkostnader",
        "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075",
        "assetType": "Preroll",
        "muted": true,
        "fullscreen": false,
        "autoplay": true,
        "mediaAssetId": "sdrn:aftonbladet:mediaasset:347346",
        "adSequenceCount": 1,
        "apiUrl": "https://svp.vg.no/svp/api/v1/ab/assets/347346?appName=linkpulse",
        "adSequenceDuration": [
            20
        ],
        "adSequencePosition": 1,
        "duration": 20,
        "category": "play > webbtv > nyheter",
        "sticky": true,
        "page": {
            "@id": "sdrn:aftonbladet:article:Rr77qd",
            "@type": "Article",
            "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075"
        }
    },
    "origin": {
        "url": "https://www.aftonbladet.se/callback?state=9c342c19ab7ea"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "spt:engage": "AB",
        "product": "hyperionX",
        "productTag": "Aftonbladet",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "intent": "Watch",
    "@type": "Engagement",
    "position": 12.0,
    "duration": 12000.0,
    "@id": "12344",
    "pageViewId": "7eb2c73a-8e49-4b3c-a5b2-1410dad44a99",
    "creationDate": "2022-09-25T18:12:04.366Z"
}""",
"""{
    "actor": {
        "@id": "s00000000-0000-4000-8000-000000000000",
        "spt:userId": "00000000-0000-4000-8000-000000000000",
        "subscriptionName": "No"
    },
    "experiments": [
        {
            "@id": "sdrn:curate:experiment:ab-conversion-ranker-imp-warmup-3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "1138x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftonbladet:frontpage:026fdb5a-626d-469e-90b1-9d7801442477",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:pageId": "026fdb5a-626d-469e-90b1-9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 33
        },
        "name": "Nyheter från Sveriges största nyhetssajt",
        "url": "https://www.aftonbladet.se/"
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "product": "hyperionX",
        "productType": "ResponsiveWeb",
        "productTag": "Aftonbladet",
        "spt:engage": "AB"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "spt:pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
    "duration": 5000,
    "scrollPosition": 0,
    "@id": "1234",
    "@type": "Engagement",
    "pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
    "creationDate": "2022-09-25T18:39:37.597Z",
    "location": null
}
"""
)

// COMMAND ----------

val  eventsSchemaIncorrect = List("""{
  "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
  "spt:pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
  "duration": 10000,
  "scrollPosition": 0,
  "@id": "61ca5ba0-d4dc-451b-aedc-70e6ae718179",
  "@type": "Engagement",
  "pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
  "creationDate": "2022-09-25T07:56:28.042Z",
  "experiments": [
    {
      "@id": "sdrn: curate:experiment: ab - conversion - ranker - imp - warmup - 3",
      "variant": "control",
      "name": "Conversion ranker test - 50k impression warmup"
    }
  ],
  "device": {
    "@type": "Device",
    "environmentId": "sdrn:schibsted:TESTING:5a54743e-e199-4f52-9ebf-07535f04b708",
    "acceptLanguage": "sv - SE",
    "screenSize": "1792x1120",
    "userAgent": "Mozilla / 5.0(Macintosh; Intel Mac OS X 10_15_7) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 105.0.0.0 Safari / 537.36",
    "deviceType": "desktop",
    "viewportSize": "1103x1016",
    "localStorageEnabled": true
  },
  "object": {
    "@id": "sdrn: aftonbladet:frontpage: 026fdb5a - 626d - 469e-90b1 - 9d7801442477",
    "@type": "Frontpage",
    "spt:custom": {
      "spt:pageId": "026fdb5a - 626d - 469e-90b1 - 9d7801442477",
      "spt:permalink": "https://www.aftonbladet.se/",
      "spt:group": "frontpage",
      "spt:site": "AB",
      "spt:segment": "0",
      "spt:dominantSection": [
        "frontpage"
      ],
      "spt:previewUrl": {
        "http": "http://www.aftonbladet.se/",
        "https": "https://www.aftonbladet.se/"
      },
      "spt:shareUrl": {
        "facebook": "https://www.aftonbladet.se/",
        "twitter": "https://www.aftonbladet.se/"
      },
      "spt:articleViewedPercentage": 40
    },
    "name": "Nyheter från Sveriges största nyhetssajt",
    "url": "https://www.aftonbladet.se/"
  },
  "origin": {
    "url": ""
  },
  "provider": {
    "@type": "Organization",
    "@id": "sdrn:schibsted:client:aftonbladet",
    "product": "hyperionX",
    "productType": "ResponsiveWeb",
    "productTag": "Aftonbladet",
    "spt:engage": "AB"
  },
  "session": {
    "@id": "sdrn:schibsted:session:faeae7a8-ba10-4fec-b786-33a6963964eb",
    "startTime": "2022-09-25T07:56:18.019Z"
  },
  "tracker": {
    "name": "Pulse Node.js SDK",
    "type": "JS",
    "eventBuilderVersion": "1.2.5",
    "version": "4.6.5-45c7a",
    "location": null
  }
}""","""{
  "experiments": [
    {
      "@id": "sdrn:aftenposten:experiment:frontpage-beta",
      "variant": "beta-user"
    },
    {
      "@id": "sdrn:aftenposten:experiment:games-widget",
      "variant": "A"
    }
  ],
  "device": {
    "@type": "Device",
    "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
    "acceptLanguage": "sv-SE",
    "screenSize": "1792x1120",
    "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "deviceType": "desktop",
    "viewportSize": "947x1016",
    "localStorageEnabled": true,
    "jweIds": "null"
  },
  "object": {
    "@id": "sdrn:aftenposten:frontpage:Forsiden",
    "@type": "Frontpage",
    "spt:custom": {
      "spt:url": "https://www.aftenposten.no/",
      "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
      "spt:device": "desktop",
      "spt:permalink": "https://www.aftenposten.no/",
      "spt:site": "aftenposten",
      "spt:pageId": "Forsiden",
      "abTestValue": "26"
    },
    "name": "Forsiden - Aftenposten",
    "url": "https://www.aftenposten.no/"
  },
  "origin": {
    "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
  },
  "provider": {
    "@type": "Organization",
    "@id": "sdrn:schibsted:client:aftenposten",
    "spt:engage": "aftenposten",
    "product": "fastenposten",
    "productType": "ResponsiveWeb"
  },
  "session": {
    "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
    "startTime": "2022-09-25T18:06:12.743Z"
  },
  "tracker": {
    "name": "Pulse Node.js SDK",
    "type": "JS",
    "eventBuilderVersion": "1.2.8",
    "version": "4.7.1-109b3"
  },
  "@type": "View",
  "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
  "creationDate": "2022-09-25T18:06:44.687Z",
  "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",
                                """{
    "experiments": [
  {
    "@id": "sdrn:aftenposten:experiment:frontpage-beta",
    "variant": "beta-user"
  },
  {
    "@id": "sdrn:aftenposten:experiment:games-widget",
    "variant": "A"
  }
],
"device": {
  "@type": "Device",
  "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
  "acceptLanguage": "sv-SE",
  "screenSize": "1792x1120",
  "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
  "deviceType": "desktop",
  "viewportSize": "947x1016",
  "localStorageEnabled": true,
  "jweIds": "null"
},
"object": {
  "@id": "sdrn:aftenposten:frontpageS:Forsiden",
  "@type": "Frontpage",
  "spt:custom": {
    "spt:url": "https://www.aftenposten.no/",
    "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
    "spt:device": "desktop",
    "spt:permalink": "https://www.aftenposten.no/",
    "spt:site": "aftenposten",
    "spt:pageId": "Forsiden",
    "abTestValue": "26"
  },
  "name": "Forsiden - Aftenposten",
  "url": "https://www.aftenposten.no/"
},
"origin": {
  "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
},
"provider": {
  "@type": "Organization",
  "@id": "sdrn:schibsted:client:aftenposten",
  "spt:engage": "aftenposten",
  "product": "fastenposten",
  "productType": "ResponsiveWeb"
},
"session": {
  "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
  "startTime": "2022-09-25T18:06:12.743Z"
},
"tracker": {
  "name": "Pulse Node.js SDK",
  "type": "JS",
  "eventBuilderVersion": "1.2.8",
  "version": "4.7.1-109b3"
},
"@id": "12344",
"@type": "View",
"pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
"creationDate": "2022-09-25T18:06:44.687Z",
"schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",
                                 """{
  "actor": {
    "@id": "00000000-0000-4000-8000-000000000000",
    "spt:userId": "1234",
    "subscriptionName": "No"
  },
  "experiments": [
    {
      "@id": "sdrn:curate:experiment:ab-conversion-ranker-imp-warmup-3",
      "variant": "control",
      "name": "Conversion ranker test - 50k impression warmup"
    }
  ],
  "device": {
    "@type": "Device",
    "environmentId": "00000000-0000-4000-8000-000000000000",
    "acceptLanguage": "sv-SE",
    "screenSize": "1792x1120",
    "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "deviceType": "desktop",
    "viewportSize": "1138x1016",
    "localStorageEnabled": true,
    "jweIds": 1234
  },
  "object": {
    "@id": "sdrn:aftonbladet:frontpage:026fdb5a-626d-469e-90b1-9d7801442477",
    "@type": "Frontpage",
    "spt:custom": {
      "spt:pageId": "026fdb5a-626d-469e-90b1-9d7801442477",
      "spt:permalink": "https://www.aftonbladet.se/",
      "spt:group": "frontpage",
      "spt:site": "AB",
      "spt:segment": "0",
      "spt:dominantSection": [
        "frontpage"
      ],
      "spt:previewUrl": {
        "http": "http://www.aftonbladet.se/",
        "https": "https://www.aftonbladet.se/"
      },
      "spt:shareUrl": {
        "facebook": "https://www.aftonbladet.se/",
        "twitter": "https://www.aftonbladet.se/"
      },
      "spt:articleViewedPercentage": 33
    },
    "name": "Nyheter från Sveriges största nyhetssajt",
    "url": "https://www.aftonbladet.se/"
  },
  "origin": {
    "url": ""
  },
  "provider": {
    "@type": "Organization",
    "@id": "sdrn:schibsted:client:aftonbladet",
    "product": "hyperionX",
    "productType": "ResponsiveWeb",
    "productTag": "Aftonbladet",
    "spt:engage": "AB"
  },
  "session": {
    "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
    "startTime": "2022-09-25T18:09:12.642Z"
  },
  "tracker": {
    "name": "Pulse Node.js SDK",
    "type": "JS",
    "eventBuilderVersion": "1.2.5",
    "version": "4.6.5-45c7a"
  },
  "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
  "spt:pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
  "duration": 5000,
  "scrollPosition": 0,
  "@id": "1234",
  "@type": "Engagement",
  "pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
  "creationDate": "2022-09-25T18:39:37.597Z",
  "location": null
}""")

// COMMAND ----------

//1 test no category, no tags -> should pass
//2 test single category, multiple tags -> should pass
//3 test multiple category with tags -> should pass
//4 test multiple category separeted by , and it has tags -> should pass
//5 tests OK query
 

val eventsATECorrect = List("""
    {"schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "spt:pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "duration": 10000,
    "scrollPosition": 0,
    "@id": "61ca5ba0-d4dc-451b-aedc-70e6ae718179",
    "@type": "Engagement",
    "pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "creationDate": "2022-09-25T07:56:28.042Z",
    "experiments": [
        {
            "@id": "sdrn: curate:experiment: ab - conversion - ranker - imp - warmup - 3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv - SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla / 5.0(Macintosh; Intel Mac OS X 10_15_7) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 105.0.0.0 Safari / 537.36",
        "deviceType": "desktop",
        "viewportSize": "1103x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn: aftonbladet:frontpage: 026fdb5a - 626d - 469e-90b1 - 9d7801442477",
        "@type": "Frontpage",
        "spt:custom": {
            "spt:pageId": "026fdb5a - 626d - 469e-90b1 - 9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 40
        },
        "name": "Nyheter från Sveriges största nyhetssajt",
        "url": "https://www.aftonbladet.se/"
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "product": "hyperionX",
        "productType": "ResponsiveWeb",
        "productTag": "Aftonbladet",
        "spt:engage": "AB"
    },
    "session": {
        "@id": "sdrn:schibsted:session:faeae7a8-ba10-4fec-b786-33a6963964eb",
        "startTime": "2022-09-25T07:56:18.019Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    }
}""",











"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Article",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": [
            "Nord Stream",
            "Östersjön",
            "Ryssland",
            "Gasläckorna på Nord Stream 1 och 2"
        ],
        "category": "Nyheter"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",





"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Article",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": [
            "Nord Stream",
            "Östersjön",
            "Ryssland",
            "Gasläckorna på Nord Stream 1 och 2"
        ],
        "category": "Bap > Bap-sale > Fritid, Hobby og Underholdning > Bøker og blader > Skjønnlitteratur"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",











"""{
    "actor": {
        "@id": "00000000-0000-4000-8000-000000000000",
        "spt:userId": "00000000-0000-4000-8000-000000000000",
        "subscriptionName": "No"
    },
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null",
        "additionalDeviceIds": null
    },
    "object": {
        "@id": "sdrn:aftonbladet:mediaad:382258706",
        "@type": "ClassifiedAd",
        "name": "Aftonbladet Direkt - Skidorter oroade över höga elkostnader",
        "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075",
        "assetType": "Preroll",
        "muted": true,
        "fullscreen": false,
        "autoplay": true,
        "mediaAssetId": "sdrn:aftonbladet:mediaasset:347346",
        "adSequenceCount": 1,
        "apiUrl": "https://svp.vg.no/svp/api/v1/ab/assets/347346?appName=linkpulse",
        "adSequenceDuration": [
            20
        ],
        "adSequencePosition": 1,
        "duration": 20,
        "category": "Play > Webbtv > Nyheter, A > B",
        "sticky": true,
        "page": {
            "@id": "sdrn:aftonbladet:article:Rr77qd",
            "@type": "Article",
            "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075"
        }
    },
    "origin": {
        "url": "https://www.aftonbladet.se/callback?state=9c342c19ab7ea"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "spt:engage": "AB",
        "product": "hyperionX",
        "productTag": "Aftonbladet",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "intent": "Watch",
    "@type": "View",
    "position": 12.0,
    "duration": 12000.0,
    "@id": "12344",
    "pageViewId": "7eb2c73a-8e49-4b3c-a5b2-1410dad44a99",
    "creationDate": "2022-09-25T18:12:04.366Z"
}""",










"""{
    "actor": {
        "@id": "s00000000-0000-4000-8000-000000000000",
        "spt:userId": "00000000-0000-4000-8000-000000000000",
        "subscriptionName": "No"
    },
    "experiments": [
        {
            "@id": "sdrn:curate:experiment:ab-conversion-ranker-imp-warmup-3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "1138x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftonbladet:frontpage:026fdb5a-626d-469e-90b1-9d7801442477",
        "@type": "Listing",
        "spt:custom": {
            "spt:pageId": "026fdb5a-626d-469e-90b1-9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 33
        },
        "name": "Nyheter från Sveriges största nyhetssajt",
        "url": "https://www.aftonbladet.se/",
        "filters": {
            "query": "This is a query"
        }
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "product": "hyperionX",
        "productType": "ResponsiveWeb",
        "productTag": "Aftonbladet",
        "spt:engage": "AB"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "spt:pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
    "duration": 5000,
    "scrollPosition": 0,
    "@id": "1234",
    "@type": "View",
    "pageViewId": "745d60ee-fa12-4ced-8820-451e11fbf014",
    "creationDate": "2022-09-25T18:39:37.597Z",
    "location": null
}
"""
)



// COMMAND ----------

// 1 case: category missing 
// 2 case: category not a string
// 3 case: category Regex A > b
// 4 case: category Regex A>B + tags are missing
// 5 case: category Regex a,b > C + Not string
// 6 case: category Regex A > 123 + Tags not an array
// 7 case: category Regex A > B > A + tags are missing
// 8 case: Query missing
// 9 case: Query not a string

 

val eventsATEIncorrect = List("""
    {"schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "spt:pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "duration": 10000,
    "scrollPosition": 0,
    "@id": "61ca5ba0-d4dc-451b-aedc-70e6ae718179",
    "@type": "View",
    "pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
    "creationDate": "2022-09-25T07:56:28.042Z",
    "experiments": [
        {
            "@id": "sdrn: curate:experiment: ab - conversion - ranker - imp - warmup - 3",
            "variant": "control",
            "name": "Conversion ranker test - 50k impression warmup"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv - SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla / 5.0(Macintosh; Intel Mac OS X 10_15_7) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 105.0.0.0 Safari / 537.36",
        "deviceType": "desktop",
        "viewportSize": "1103x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn: aftonbladet:frontpage: 026fdb5a - 626d - 469e-90b1 - 9d7801442477",
        "@type": "ClassifiedAd",
        "spt:custom": {
            "spt:pageId": "026fdb5a - 626d - 469e-90b1 - 9d7801442477",
            "spt:permalink": "https://www.aftonbladet.se/",
            "spt:group": "frontpage",
            "spt:site": "AB",
            "spt:segment": "0",
            "spt:dominantSection": [
                "frontpage"
            ],
            "spt:previewUrl": {
                "http": "http://www.aftonbladet.se/",
                "https": "https://www.aftonbladet.se/"
            },
            "spt:shareUrl": {
                "facebook": "https://www.aftonbladet.se/",
                "twitter": "https://www.aftonbladet.se/"
            },
            "spt:articleViewedPercentage": 40
        },
        "name": "Nyheter från Sveriges största nyhetssajt",
        "url": "https://www.aftonbladet.se/"
    },
    "origin": {
        "url": ""
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "product": "hyperionX",
        "productType": "ResponsiveWeb",
        "productTag": "Aftonbladet",
        "spt:engage": "AB"
    },
    "session": {
        "@id": "sdrn:schibsted:session:faeae7a8-ba10-4fec-b786-33a6963964eb",
        "startTime": "2022-09-25T07:56:18.019Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    }
}""",











"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Article",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": [
            "Nord Stream",
            "Östersjön",
            "Ryssland",
            "Gasläckorna på Nord Stream 1 och 2"
        ],
        "category": 123
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",










"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Article",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": [
            "Nord Stream",
            "Östersjön",
            "Ryssland",
            "Gasläckorna på Nord Stream 1 och 2"
        ],
        "category": "A > b"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",







"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Article",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": [
            "Nord Stream",
            "Östersjön",
            "Ryssland",
            "Gasläckorna på Nord Stream 1 och 2"
        ],
        "category": "A>B"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",





"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Article",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "category": "A,a > B"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",





"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Article",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": [
            1,
            2
        ],
        "category": "A > 123"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",







"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Article",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": "TEST",
        "category": "A > B > A"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""",









"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Listing",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": [
            "Nord Stream",
            "Östersjön",
            "Ryssland",
            "Gasläckorna på Nord Stream 1 och 2"
        ],
        "category": "Bap > Bap-sale > Fritid, Hobby og Underholdning > Bøker og blader > Skjønnlitteratur"
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}
""",





"""{
    "experiments": [
        {
            "@id": "sdrn:aftenposten:experiment:frontpage-beta",
            "variant": "beta-user"
        },
        {
            "@id": "sdrn:aftenposten:experiment:games-widget",
            "variant": "A"
        }
    ],
    "device": {
        "@type": "Device",
        "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null"
    },
    "object": {
        "@id": "sdrn:aftenposten:frontpage:Forsiden",
        "@type": "Listing",
        "spt:custom": {
            "spt:url": "https://www.aftenposten.no/",
            "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
            "spt:device": "desktop",
            "spt:permalink": "https://www.aftenposten.no/",
            "spt:site": "aftenposten",
            "spt:pageId": "Forsiden",
            "abTestValue": "26"
        },
        "name": "Forsiden - Aftenposten",
        "url": "https://www.aftenposten.no/",
        "tags": [
            "Nord Stream",
            "Östersjön",
            "Ryssland",
            "Gasläckorna på Nord Stream 1 och 2"
        ],
        "category": "Bap > Bap-sale > Fritid, Hobby og Underholdning > Bøker og blader > Skjønnlitteratur",
        "filters": {
            "query": 123
        }
    },
    "origin": {
        "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftenposten",
        "spt:engage": "aftenposten",
        "product": "fastenposten",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
        "startTime": "2022-09-25T18:06:12.743Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.8",
        "version": "4.7.1-109b3"
    },
    "@id": "12344",
    "@type": "View",
    "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
    "creationDate": "2022-09-25T18:06:44.687Z",
    "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}
"""
)

// COMMAND ----------

// 1 case: correct schema + + contains all CAM fields +  correct tags + correct categories  (0,0,0,0,1)
// 2 case: correct schema + contains all CAM + correct tags but incorrect categories (0,0,0,0,1)
// 3 case: incorrect schema + inccorect CAM + correct query  : (1,1,0,0,1)
// 4 case: incorrect schema + incorrect CAM + incorrect tags and categories : (1,1,1,1,0,1)
val eventsTest = List("""
{
  "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
  "spt:pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
  "duration": 10000,
  "scrollPosition": 0,
  "@id": "61ca5ba0-d4dc-451b-aedc-70e6ae718179",
  "@type": "View",
  "pageViewId": "a12b89ec-ace0-4a97-84ea-5bde7725c709",
  "creationDate": "2022-09-25T07:56:28.042Z",
  "experiments": [
    {
      "@id": "sdrn: curate:experiment: ab - conversion - ranker - imp - warmup - 3",
      "variant": "control",
      "name": "Conversion ranker test - 50k impression warmup"
    }
  ],
  "device": {
    "@type": "Device",
    "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
    "acceptLanguage": "sv - SE",
    "screenSize": "1792x1120",
    "userAgent": "Mozilla / 5.0(Macintosh; Intel Mac OS X 10_15_7) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 105.0.0.0 Safari / 537.36",
    "deviceType": "desktop",
    "viewportSize": "1103x1016",
    "localStorageEnabled": true,
    "jweIds": "null"
  },
  "object": {
    "@id": "sdrn:aftonbladet:article:026fdb5a-626d-469e-90b1-9d7801442477",
    "@type": "Article",
    "spt:custom": {
      "spt:pageId": "026fdb5a - 626d - 469e-90b1 - 9d7801442477",
      "spt:permalink": "https://www.aftonbladet.se/",
      "spt:group": "article",
      "spt:site": "AB",
      "spt:segment": "0",
      "spt:dominantSection": [
        "frontpage"
      ],
      "spt:previewUrl": {
        "http": "http://www.aftonbladet.se/",
        "https": "https://www.aftonbladet.se/"
      },
      "spt:shareUrl": {
        "facebook": "https://www.aftonbladet.se/",
        "twitter": "https://www.aftonbladet.se/"
      },
      "spt:articleViewedPercentage": 40
    },
    "name": "Nyheter från Sveriges största nyhetssajt",
    "url": "https://www.aftonbladet.se/"
  },
  "origin": {
    "url": ""
  },
  "provider": {
    "@type": "Organization",
    "@id": "sdrn:schibsted:client:aftonbladet",
    "product": "hyperionX",
    "productType": "ResponsiveWeb",
    "productTag": "Aftonbladet",
    "spt:engage": "AB"
  },
  "session": {
    "@id": "sdrn:schibsted:session:faeae7a8-ba10-4fec-b786-33a6963964eb",
    "startTime": "2022-09-25T07:56:18.019Z"
  },
  "tracker": {
    "name": "Pulse Node.js SDK",
    "type": "JS",
    "eventBuilderVersion": "1.2.5",
    "version": "4.6.5-45c7a"
  }
}""",
                  
                  
                  
                  
                  
                  
                  
         """{
  "experiments": [
    {
      "@id": "sdrn:aftenposten:experiment:frontpage-beta",
      "variant": "beta-user"
    },
    {
      "@id": "sdrn:aftenposten:experiment:games-widget",
      "variant": "A"
    }
  ],
  "device": {
    "@type": "Device",
    "environmentId": "sdrn:schibsted:environment:5a54743e-e199-4f52-9ebf-07535f04b708",
    "acceptLanguage": "sv-SE",
    "screenSize": "1792x1120",
    "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "deviceType": "desktop",
    "viewportSize": "947x1016",
    "localStorageEnabled": true,
    "jweIds": "null"
  },
  "object": {
    "@id": "sdrn:aftenposten:article:Forsiden",
    "@type": "Article",
    "spt:custom": {
      "spt:url": "https://www.aftenposten.no/",
      "spt:referrer": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger",
      "spt:device": "desktop",
      "spt:permalink": "https://www.aftenposten.no/",
      "spt:site": "aftenposten",
      "spt:pageId": "Forsiden",
      "abTestValue": "26"
    },
    "name": "Forsiden - Aftenposten",
    "url": "https://www.aftenposten.no/",
    "tags": [
      "Nord Stream",
      "Östersjön",
      "Ryssland",
      "Gasläckorna på Nord Stream 1 och 2"
    ],
    "category": "Testing>Fail"
  },
  "origin": {
    "url": "https://www.aftenposten.no/verden/i/Bj5MK9/igjen-og-igjen-ble-putins-planer-avsloert-det-kan-fortelle-noe-viktig-om-fremtidens-kriger"
  },
  "provider": {
    "@type": "Organization",
    "@id": "sdrn:schibsted:client:aftenposten",
    "spt:engage": "aftenposten",
    "product": "fastenposten",
    "productType": "ResponsiveWeb"
  },
  "session": {
    "@id": "sdrn:schibsted:session:de77b724-7e6c-4772-941e-1cde9eeb83d8",
    "startTime": "2022-09-25T18:06:12.743Z"
  },
  "tracker": {
    "name": "Pulse Node.js SDK",
    "type": "JS",
    "eventBuilderVersion": "1.2.8",
    "version": "4.7.1-109b3"
  },
  "@id": "12344",
  "@type": "View",
  "pageViewId": "3f12f97a-d8d7-472c-b035-53be0f8190e5",
  "creationDate": "2022-09-25T18:06:44.687Z",
  "schema": "http://schema.schibsted.com/events/tracker-event.json/236.json"
}""", 
                  
                  
                  """{
    "actor": {
    "test": "fail"
    },
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null",
        "additionalDeviceIds": null
    },
    "object": {
        "@id": "sdrn:aftonbladet:listing:382258706",
        "@type": "Listing",
        "name": "Aftonbladet Direkt - Skidorter oroade över höga elkostnader",
        "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075",
        "assetType": "Preroll",
        "muted": true,
        "fullscreen": false,
        "autoplay": true,
        "mediaAssetId": "sdrn:aftonbladet:mediaasset:347346",
        "adSequenceCount": 1,
        "apiUrl": "https://svp.vg.no/svp/api/v1/ab/assets/347346?appName=linkpulse",
        "adSequenceDuration": [
            20
        ],
        "adSequencePosition": 1,
        "duration": 20,
        "category": "play > webbtv > nyheter",
        "sticky": true,
        "page": {
            "@id": "sdrn:aftonbladet:article:Rr77qd",
            "@type": "Article",
            "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075"
        },
        "filters": {
            "query": "This is a query"
        }
    },
    "origin": {
        "url": "https://www.aftonbladet.se/callback?state=9c342c19ab7ea"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "spt:engage": "AB",
        "product": "hyperionX",
        "productTag": "Aftonbladet",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "intent": "Watch",
    "@type": "View",
    "position": 12.0,
    "duration": 12000.0,
    "@id": "12344",
    "pageViewId": "7eb2c73a-8e49-4b3c-a5b2-1410dad44a99",
    "creationDate": "2022-09-25T18:12:04.366Z"
}""",
                  
                  
                  """{
    "actor": {
        "@idF": "00000000-0000-4000-8000-000000000000",
        "spt:userIdF": "00000000-0000-4000-8000-000000000000",
        "subscriptionNameF": "No"
    },
    "device": {
        "@type": "Device",
        "environmentId": "00000000-0000-4000-8000-000000000000",
        "acceptLanguage": "sv-SE",
        "screenSize": "1792x1120",
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "deviceType": "desktop",
        "viewportSize": "947x1016",
        "localStorageEnabled": true,
        "jweIds": "null",
        "additionalDeviceIds": null
    },
    "object": {
        "@id": "sdrn:aftonbladet:article:382258706",
        "@type": "Article",
        "name": "Aftonbladet Direkt - Skidorter oroade över höga elkostnader",
        "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075",
        "assetType": "Preroll",
        "muted": true,
        "fullscreen": false,
        "autoplay": true,
        "mediaAssetId": "sdrn:aftonbladet:mediaasset:347346",
        "adSequenceCount": 1,
        "apiUrl": "https://svp.vg.no/svp/api/v1/ab/assets/347346?appName=linkpulse",
        "adSequenceDuration": [
            20
        ],
        "adSequencePosition": 1,
        "duration": 20,
        "category": "play > webbtv > nyheter",
        "sticky": true,
        "page": {
            "@id": "sdrn:aftonbladet:article:Rr77qd",
            "@type": "Article",
            "url": "https://www.aftonbladet.se/nyheter/a/Rr77qd/aftonbladet-direkt?pinnedEntry=960075"
        }
    },
    "origin": {
        "url": "https://www.aftonbladet.se/callback?state=9c342c19ab7ea"
    },
    "provider": {
        "@type": "Organization",
        "@id": "sdrn:schibsted:client:aftonbladet",
        "spt:engage": "AB",
        "product": "hyperionX",
        "productTag": "Aftonbladet",
        "productType": "ResponsiveWeb"
    },
    "session": {
        "@id": "sdrn:schibsted:session:26d6ae27-cb31-453c-8aee-9f5ee98e40f8",
        "startTime": "2022-09-25T18:09:12.642Z"
    },
    "tracker": {
        "name": "Pulse Node.js SDK",
        "type": "JS",
        "eventBuilderVersion": "1.2.5",
        "version": "4.6.5-45c7a"
    },
    "schema": "http://schema.schibsted.com/events/tracker-event.json/286.json",
    "intent": "Watch",
    "@type": "View",
    "position": 12.0,
    "duration": 12000.0,
    "@id": "12344",
    "pageViewId": "7eb2c73a-8e49-4b3c-a5b2-1410dad44a99",
    "creationDate": "2022-09-25T18:12:04.366Z"
}"""
)

// COMMAND ----------

val error = List((1, 0, 1, 1, 0, 1), (0, 0, 1, 0, 0, 1), (1, 1, 0, 0, 0, 1), (1, 1, 1, 1, 0, 1))
def testDQcheck(): Unit = {
  assert(eventsTest.map(runChecksOnEvent(_)) == error)
}

testDQcheck()


// COMMAND ----------

object testSchema {
  def runSchemaCheckTest(event: String): (Int) = {
    val jsonTree = Validation.getJsonTree(event)
    val schemaErrors = SchemaCheck.checkSchema(jsonTree)
    (schemaErrors)
  }

  def runSchemaChecksOnFile(events: List[String]): (Int) = {
    events.map(runSchemaCheckTest(_)).sum
  }

  def runTestSchema(): Unit = {

    assert(runSchemaChecksOnFile(eventsSchemaCorrect) == 0)
    assert(runSchemaChecksOnFile(eventsSchemaIncorrect) == 4)
  }
}    

// COMMAND ----------

testSchema.runTestSchema()

// COMMAND ----------

object testCAM {
  def runCAMChecksOnEvent(event: String): (Int) = {
    val jsonTree = Validation.getJsonTree(event)
    val CAMErrors = CAMCheck.checkCAM(jsonTree)
    (CAMErrors)
  }

  def runCAMChecksOnFile(events: List[String]): (Int) = {
    events.map(runCAMChecksOnEvent(_)).sum
  }

  def runTestCAM(): Unit = {
    assert(runCAMChecksOnFile(eventsCorrectCAM) == 0)
    assert(runCAMChecksOnFile(eventsIncorrectCAM) == 4)
  }
}

// COMMAND ----------

object testATE {
  val errorIncorrectList = List(
    (1, 0, 0, 1),
    (1, 0, 0, 1),
    (1, 0, 0, 1),
    (1, 0, 0, 1),
    (1, 1, 0, 1),
    (1, 1, 0, 1),
    (1, 1, 0, 1),
    (0, 0, 1, 1),
    (0, 0, 1, 1))
  val errorCorrectList = List((0, 0, 0, 0),
    (0, 0, 0, 1),
    (0, 0, 0, 1),
    (0, 0, 0, 1),
    (0, 0, 0, 1))

  def runATECheckTest(event: String): (Int, Int, Int, Int) = {
    val jsonTree = Validation.getJsonTree(event)
    val ATEErrors = ATECheck.ATECheck(jsonTree)
    (ATEErrors)
  }

  def runATEChecksOnFile(events: List[String]): List[(Int, Int, Int, Int)] = {
    events.map(runATECheckTest(_))
  }

  def testATE(): Unit = {
    assert(runATEChecksOnFile(eventsATECorrect) == errorCorrectList)
    assert(runATEChecksOnFile(eventsATEIncorrect) == errorIncorrectList)
  }
}

// COMMAND ----------

testATE.testATE()

// COMMAND ----------

testSchema.runTestSchema()

// COMMAND ----------

testCAM.runTestCAM()
