val path = "/tmp/BDLE/dataset/VKRU18s/"

val vk = path + "vk_001.json"
val data = spark.read.format("json").load(vk).dropDuplicates
val total = data.count()


data.printSchema



val attrs = List("event", 
                 "event.event_id", 
                 "event.event_id.post_id", 
                 "event.event_id.post_owner_id", 
                 "event.event_id.comment_id", 
                 "event.event_id.shared_post_id", 
                 "event.author", 
                 "event.attachments", 
                 "event.geo", 
                 "event.tags",
                 "event.creation_time")

attrs.foreach(x=>println(x + " "+data.where(x+" is not null").count))


//applatissement des listes de tag et construction d'une colonne avec
val dataWithTags = data.withColumn("tag", explode($"event.tags"))



//Retourner le nombre de posts par tag


val postsPerTag = dataWithTags.groupBy($"tag").agg(countDistinct("event.event_id.post_id") as "count")



val authCountPerTag = dataWithTags.groupBy($"tag").agg(countDistinct("event.author.id") as "nbAuths")





import spark.implicits._

case class Vote(name: String, party: String, votes: Long)
val votes = Seq(Vote("putin", "Independent", 56430712), 
                Vote("grudinin", "Communist",8659206), 
                Vote("zhirinovsky","Liberal Democratic Party",4154985),
                Vote("sobchak","Civic Initiative",1238031),
                Vote("yavlinsky","Yabloko",769644), 
                Vote("titov","Party of Growth",556801)).toDS()
votes.printSchema
votes.show()






val votesCount = votes.as("t1").join(authCountPerTag.as("t2"),$"t1.name"===$"t2.tag").groupBy("name").agg(sum("votes") as "votes",sum("nbAuths") as "nbAuths")


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val ord_Auth = Window.orderBy($"nbAuths".desc)
val ord_votes= Window.orderBy($"votes".desc)

val res_final=votesCount.withColumn("rank_votes",rank().over(ord_votes)).withColumn("rank_nbAuths",rank().over(ord_Auth))



val dataTagMon = dataWithTags.withColumn("month",month(from_unixtime($"event.creation_time")))



val cub_ev_tag_mo =  dataTagMon.rollup("event.event_type", "tag", "month").count()
val cub_ev_tag_mo_notnull = cub_ev_tag_mo.where("event_type is not null and tag is not null and month is not null")



val monthEvent = cub_ev_tag_mo_notnull.cube($"month",$"event_type").agg(sum("count") as "nb_post").where("event_type is not null and month is not null")

val monthEvent_pivoted=monthEvent.groupBy("month").pivot("event_type").agg(sum("nb_post"))





val authTag = dataWithTags.groupBy($"tag").agg(collect_set(concat_ws(":",$"event.author.id",$"event.creation_time")) as "Authors")

import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.collection.mutable.WrappedArray

def commonAuthors: UserDefinedFunction =  udf((l: WrappedArray[Integer], r: WrappedArray[Integer]) => {l.intersect(r).size})


val tagCoOcc=authTag.as("t1").join(authTag.as("t2"),$"t1.tag">$"t2.tag").withColumn("count",commonAuthors($"t1.Authors",$"t2.Authors")).select($"t1.tag" as "tag",$"t2.tag" as "OtherTag",$"count").orderBy($"count".desc)

val tagMat = tagCoOcc.groupBy("tag").pivot("otherTag").agg(sum("count"))



