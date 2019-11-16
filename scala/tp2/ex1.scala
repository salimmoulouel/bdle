val path = "/tmp/BDLE/dataset/Books/" 


val users_df = spark.read.format("csv").
option("header", "true").option("inferSchema", "true").load(path +"users.csv")

val books_df = spark.read.format("csv").
option("header", "true").option("inferSchema", "true").load(path +"books.csv")

val ratings_df = spark.read.format("csv").
option("header", "true").option("inferSchema", "true").load(path +"ratings.csv")

var s0=users_df.where($"country"==="france")

var s1=books_df.where($"year"===2000)


val s2 = ratings_df.where($"rating">3)


val q1 = users_df.groupBy($"country").agg(countDistinct("userid") as "nb_user")

val q2=q1.orderBy($"nb_user".desc)


val q3 = books_df.groupBy($"year").agg(countDistinct("bookid") as "nb_livres").orderBy($"nb_livres")


val q4 = books_df.groupBy($"publisher").agg(countDistinct("bookid") as "nb_livres").filter(!$"nb_livres".isNull).filter($"nb_livres">10)

val q5 = books_df.groupBy($"publisher",$"year").agg(countDistinct("bookid") as "nb_livres").filter(!$"nb_livres".isNull).filter($"nb_livres">5).orderBy($"year".desc,$"publisher".desc)



val q6 = ratings_df.groupBy($"bookid").agg(avg("rating"))

val q = ratings_df.filter($"rating"===5).count()

val q8 = ratings_df.groupBy($"userid").agg(count("bookid") as "nb_livres").orderBy($"nb_livres".desc)

//requetes avec jointures

val q9 = users_df.filter($"country"==="france").join(ratings_df,"userid").join(books_df,"bookid").select($"publisher")


val q10 = books_df.select($"publisher").except(q9)

//val q11 = ratings_df.books_df("bookid").join(users_df,"userid").groupBy($"country")val 

val q12 = ratings_df.join(users_df,"userid").groupBy($"bookid").agg(avg("age"))


val q13a = ratings_df.groupBy("userid").agg(count("bookid") as "nb_livres")


val q13 = q13a.as("t1").join(q13a.as("t2"),$"t1.nb_livres"===$"t2.nb_livres" && $"t1.userid">$"t2.userid" ).select($"t1.userid" as "u1",$"t2.userid" as "u2")


//requetes avec fonctions utilisateurs

import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.collection.mutable.WrappedArray


def commonBooks: UserDefinedFunction =  udf((l: WrappedArray[Integer], r: WrappedArray[Integer]) => {l.intersect(r).size})


def unionBooks: UserDefinedFunction =  udf((l: WrappedArray[Integer], r: WrappedArray[Integer]) => {l.union(r).size})

val q14a= ratings_df.groupBy("userid").agg(collect_list($"bookid") as "cnt_books1")

val q14b= ratings_df.groupBy("userid").agg(collect_list($"bookid") as "cnt_books2")
//ratings_df.as("t1").join(ratings_df.as("t2"),$"t1.userid">$)
val q14=q14a.as("t1").join(q14b.as("t2"),$"t1.userid">$"t2.userid").withColumn("union",commonBooks($"cnt_books1",$"cnt_books2"))


q14.where("union!='0'").show(25)


