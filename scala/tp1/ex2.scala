val path = "/tmp/BDLE/dataset/Books/" 

val books_data = sc.textFile(path + "books.csv")
val users_data = sc.textFile(path + "users.csv")
val ratings_data = sc.textFile(path + "ratings.csv")

/*
books_data.take(10)
users_data.take(10)
ratings_data.take(10)
*/
// construction des ensemble de RDD
case class User(userid: Int, country: String, age: Int) {
    override def toString: String = "Users(" + userid + "," + country + "," + age + ")"
}

case class Book(bookid: Int, titlewords: Int, authorwords: Int, year: Int, publisher: Int)  {
    override def toString: String = "Books(" + bookid + "," + titlewords + "," +  authorwords + "," + //
    year + "," + publisher + ")"
} 
case class Rating(userid: Int, bookid: Int, rating: Int) {
    override def toString: String = "Rating(" + userid + "," + bookid + "," + rating + ")"
}



val users = users_data.filter(!_.contains("userid")).map(x=>x.split(",")).map{case Array(u,c,a)=>User(u.toInt,c,a.toInt)}

val books = books_data.filter(!_.contains("bookid")).map(x=>x.split(",")).map{case Array(u,c,a,b,d)=>Book(u.toInt,c.toInt,a.toInt,b.toInt,d.toInt)}

val ratings = ratings_data.filter(!_.contains("userid")).map(x=>x.split(",")).map{case Array(u,c,a)=>Rating(u.toInt,c.toInt,a.toInt)}



//requetes sur tables

val s0=users.filter({case User(id,country,age)=>country=="france"})


val s1 = books.filter(x=>x.year==2000)

val s2 = ratings.filter(x=>x.rating>3)

//on peut faiire un filter(x=>x.isInstanceOf[Objet])

val q1 = users.map(x=>(x.country,1)).reduceByKey(_+_)

val q2=q1.sortBy(x=>(-x._2))

//requete avec jointures


val tmps0=s0.map(x=>(x.userid,1))

val tmprat=ratings.map(x=>(x.userid,x.bookid))

val join_rat_user=tmps0.join(tmprat).map(x=>(x._2._2,1))

val tmpbook=books.map(x=>(x.bookid,x.publisher))

val join_final=tmpbook.join(join_rat_user)

val q4=join_final.map(x=>x._2._1).distinct()


val q5=books.map(x=>x.publisher).subtract(q4)


val tmprat=ratings.map(x=>(x.bookid,x))
val tmpbook=books.map(x=>(x.bookid,x))

val join_rat_book=tmprat.join(tmpbook).map(x=>(x._2._1.userid ,x._2._2.bookid))

val tmpuser=users.map(x=>(x.userid,x))


val tmp_book_age = tmpuser.join(join_rat_book).map(x=>(x._2._2,x._2._1.age))

val nb_elem_key=tmp_book_age.reduceByKey( (x,y) => 1+1)

val total_age=tmp_book_age.reduceByKey( (x,y) => x+y)


val q6=nb_elem_key.join(total_age).map({ case (x,(a,b)) =>(x,b/a)})


