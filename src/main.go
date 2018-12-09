package main

import (
	"github.com/kataras/iris"

	"github.com/kataras/iris/middleware/logger"
	"github.com/kataras/iris/middleware/recover"
	"time"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/bson"
  "fmt"
 "context"
 "log"
)

func findOne() string {
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("SELECT *  FROM  rentals where id = 6000")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	
	
	
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()
}
func findAll() string{
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("SELECT *  FROM  rentals ")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//UPDATE films SET name='duongdz' WHERE id = 23
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()
}
func updateOne() string{
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
	
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("UPDATE rentals SET status=1 WHERE id = 10000 ")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//UPDATE films SET name='duongdz' WHERE id = 23
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()

}
func updateMore()string{
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("UPDATE rentals SET status=1 WHERE id<10000")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//UPDATE films SET name='duongdz' WHERE id = 23
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()
}
func findTwoTable() string{
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("Select *from rentals join users on rentals.id_user=users.id where rentals.id =50000")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//UPDATE films SET name='duongdz' WHERE id = 23
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()
}
func findThreeTable() string{
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("Select *from (rentals join users on rentals.id_user=users.id) join films on rentals.id_film = films.id where rentals.id =50000")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//UPDATE films SET name='duongdz' WHERE id = 23
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()
}
func count() string{
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("Select count(*) from rentals")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//UPDATE films SET name='duongdz' WHERE id = 23
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()
}
func deleteOne() string{
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("Delete from rentals where id =2000")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//UPDATE films SET name='duongdz' WHERE id = 23
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()
}
func deleteMore() string{
	db, err := sql.Open("mysql","root:13081999@tcp(127.0.0.1:3306)/giant_db")


	if err != nil {
		panic(err.Error())
		
	}
	fmt.Printf("start")
	start := time.Now()
    results,err := db.Query("Delete from rentals where id_user =200")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//UPDATE films SET name='duongdz' WHERE id = 23
	for results.Next() {
	}
	end :=time.Now();
	diff := end.Sub(start);
	defer db.Close()
	return diff.String()
}
func findAllM() string{
	
	 ctx, _ := context.WithTimeout(context.Background(),3600*time.Second)
	client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
	collection := client.Database("giant_db").Collection("rentals")
	if err!=nil{
		fmt.Printf("err")
	}
	i:=0;
	fmt.Printf("start")
	start := time.Now()
	cur, err := collection.Find(ctx, nil)
	if err != nil { log.Fatal(err) }
	defer cur.Close(ctx)
	for cur.Next(ctx){
i++;
	}

	end := time.Now()
	fmt.Println(i)
	diff := end.Sub(start);
	if err != nil {
		log.Fatal(err)
	}
	return diff.String()
}
func findOneM() string{
	
	ctx, _ := context.WithTimeout(context.Background(),5*time.Second)
   client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
   collection := client.Database("giant_db").Collection("rentals")
   if err!=nil{
	   fmt.Printf("err")
   }
   fmt.Printf("start")
   fiter :=bson.M{"id" : "5000" }
   start := time.Now()
  
   cur:= collection.FindOne(context.Background(), fiter )
   end := time.Now()
   if err != nil { log.Fatal(err) }
    _=cur
   
   
   diff := end.Sub(start);
   if err != nil {
	   log.Fatal(err)
   }
   return diff.String()
}
func deleteOneM() string{

	ctx, _ := context.WithTimeout(context.Background(),5*time.Second)
   client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
   collection := client.Database("giant_db").Collection("rentals")
   if err!=nil{
	   fmt.Printf("err")
   }
   fmt.Printf("start")
   fiter :=bson.M{"id" : 99 }
   start := time.Now()
  
   cur,err:= collection.DeleteOne(context.Background(), fiter )
   if err != nil { log.Fatal(err) }
    _=cur
   
   end := time.Now()
   diff := end.Sub(start);
   if err != nil {
	   log.Fatal(err)
   }
   return diff.String()	
}
func deleteMoreM() string{

	ctx, _ := context.WithTimeout(context.Background(),5*time.Second)
   client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
   collection := client.Database("giant_db").Collection("rentals")
   if err!=nil{
	   fmt.Printf("err")
   }
   fmt.Printf("start")
   fiter :=bson.M{"id_user" : 1 }
   start := time.Now()
  
   cur,err:= collection.DeleteMany(context.Background(), fiter )
   if err != nil { log.Fatal(err) }
    _=cur
   
   end := time.Now()
   diff := end.Sub(start);
   if err != nil {
	   log.Fatal(err)
   }
   return diff.String()	
}
func updateOneM() string{

	ctx, _ := context.WithTimeout(context.Background(),5*time.Second)
   client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
   collection := client.Database("giant_db").Collection("rentals")
   if err!=nil{
	   fmt.Printf("err")
   }
   fmt.Printf("start")
    
   fiter :=bson.M{"id" : 502 }
   op :=bson.M{"$set" : bson.M{"status;": 1}  }
   start := time.Now()
   
   cur,err:= collection.UpdateOne(context.Background(), fiter, op)
   if err != nil { log.Fatal(err) }
    _=cur
   
   end := time.Now()
   diff := end.Sub(start);
   if err != nil {
	   log.Fatal(err)
   }
   return diff.String()	
}
func  updateMoreM()string{

	ctx, _ := context.WithTimeout(context.Background(),5*time.Second)
   client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
   collection := client.Database("giant_db").Collection("rentals")
   if err!=nil{
	   fmt.Printf("err")
   }
   fmt.Printf("start")
   fiter :=bson.M{ "id_user" : 502 }
   start := time.Now()
   op :=bson.M{"$set" : bson.M{"status;": int8(1)} }
   cur,err:= collection.UpdateMany(context.Background(), fiter ,op)
   if err != nil { log.Fatal(err) }
    _=cur
   
   end := time.Now()
   diff := end.Sub(start);
   if err != nil {
	   log.Fatal(err)
   }
   return diff.String()	
}
func findTwoTableM() string{
	
	ctx, _ := context.WithTimeout(context.Background(),5*time.Second)
   client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
   collection := client.Database("giant_db").Collection("rentals")
   if err!=nil{
	   fmt.Printf("err")
   }
   o1 := bson.D{{"$match" ,bson.D {{"id_user",502}}}}
   o2 := bson.D{{"$lookup",  bson.M{"from": "films", "localField": "id_film", "foreignField": "id", "as": "rentals" }}}
  o3 := bson.D{ {"$unwind", "$rental"} }

  opt :=  []interface{}{o1,o2,o3}
 
   fmt.Printf("start")
  
   start := time.Now()
   
  cur, err := collection.Aggregate(context.Background(), opt)
   if err != nil { log.Fatal(err) }
    _=cur
	for cur.Next(ctx){
		fmt.Println(1)
	}
   end := time.Now()
   diff := end.Sub(start);
   if err != nil {
	   log.Fatal(err)
   }
   return diff.String()	
}
func findThreeTableM() string{
	
	ctx, _ := context.WithTimeout(context.Background(),5*time.Second)
   client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
   collection := client.Database("giant_db").Collection("rentals")
   if err!=nil{
	   fmt.Printf("err")
   }
   o1 := bson.D{{"$match" ,bson.D {{"id",502}}}}
   o2 := bson.D{{"$lookup",  bson.M{"from": "films", "localField": "id_film", "foreignField": "id", "as": "rentals" }}}
   o4 := bson.D{{"$lookup",  bson.M{"from": "users", "localField": "id_user", "foreignField": "id", "as": "rentalg" }}}
   o3 := bson.D{ {"$unwind", "$rental"} }

  opt :=  []interface{}{o1,o2,o4,o3}
 
   fmt.Printf("start")
  
   start := time.Now()
   
  cur, err := collection.Aggregate(context.Background(), opt)
   if err != nil { log.Fatal(err) }
    _=cur
	for cur.Next(ctx){
		fmt.Println(1)
	}
   end := time.Now()
   diff := end.Sub(start);
   if err != nil {
	   log.Fatal(err)
   }
   return diff.String()	
}
func countM() string {

	ctx, _ := context.WithTimeout(context.Background(),5*time.Second)
   client,err := mongo.Connect(ctx, "mongodb://localhost:27017")
   collection := client.Database("giant_db").Collection("rentals")
   if err!=nil{
	   fmt.Printf("err")
   }
   


 
   fmt.Printf("start")
  
   start := time.Now()
   
  cur, err := collection.Count(context.Background(),nil)
   if err != nil { log.Fatal(err) }
   
	
		fmt.Println("123 %d",cur)
	
   end := time.Now()
   diff := end.Sub(start);
   if err != nil {
	   log.Fatal(err)
   }
   return diff.String()	
}

func main() {
	app := iris.New()
	app.Logger().SetLevel("debug")
	app.Use(recover.New())
	app.Use(logger.New())

	views := iris.HTML("./views",".html").Layout("index.html").Reload(true);
	app.RegisterView(views)
	app.StaticWeb("/views","./views")
	app.Handle("GET", "/", func(ctx iris.Context) {
		ctx.View("index.html")
	})
	app.Get("/mysql/1", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": findOne()})
	})
	app.Get("/mysql/2", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": findAll()})
	})
	app.Get("/mysql/3", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": updateOne()})
	})
	app.Get("/mysql/4", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message":updateMore()})
	})
	
	app.Get("/mysql/5", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": deleteOne()})
	})
	app.Get("/mysql/6", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": deleteMore()})

	})
	app.Get("/mysql/7", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": count()})
		
	})
	app.Get("/mongo/1", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": findOneM()})
	})
	app.Get("/mongo/2", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": findAllM()})
	})
	app.Get("/mongo/3", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": updateOneM()})
	})
	app.Get("/mongo/4", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": updateMoreM()})
	})
	
	app.Get("/mongo/5", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": deleteOneM()})
	})
	app.Get("/mongo/6", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": deleteMoreM()})
	})
	app.Get("/mongo/7", func(ctx iris.Context) {
		ctx.JSON(iris.Map{"message": countM()})
	})









	app.Run(iris.Addr(":8080"), iris.WithoutServerError(iris.ErrServerClosed))
}