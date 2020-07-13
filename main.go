package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/ventu-io/go-shortid"
)

var Client *redis.Client

type Url struct {
	ShortURL string `json:"shorturl"`
	LongURL  string `json:"longurl"`
}

type Responsestruct struct {
	Message  string
	Response Url
}

var db *sql.DB
var err error

// Redis Connection
func Db() {

	Client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pong, err := Client.Ping(Client.Context()).Result()
	if err != nil {
		log.Fatal("Could not connect to Redis Server.")
	}
	fmt.Println(pong, "Successfully connected to Redis Server.")

}

// This function is used to store the hash key and the url into mysql tables.
func createurl(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	shorturl, err := shortid.Generate()
	stmt, err := db.Prepare("INSERT INTO url_table(shorturl,longurl) VALUES(?,?)")
	if err != nil {
		panic(err.Error())
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err.Error())
	}
	keyVal := make(map[string]string)
	json.Unmarshal(body, &keyVal)
	longurl := keyVal["longurl"]
	_, err = stmt.Exec(shorturl, longurl)
	if err != nil {
		panic(err.Error())
	}
	var responsestruct = Responsestruct{
		Message: "Short URL generated",
		Response: Url{
			LongURL:  longurl,
			ShortURL: r.Host + "/" + shorturl,
		},
	}
	jsonResponse, err := json.Marshal(responsestruct)
	w.Write(jsonResponse)

}

// This function will redirect the short url to the actual url

func Redirecturl(w http.ResponseWriter, r *http.Request) {

	shorturl := mux.Vars(r)["shorturl"]
	if shorturl == " " {
		fmt.Println("Error")
	} else {
		// Checks if key is present in Redis Server if not checks from mysql table.
		LongURL, err := Client.Get(Client.Context(), shorturl).Result()
		if err == redis.Nil {
			result, err := db.Query("SELECT longurl from url_table where shorturl = ?", shorturl)
			if err != nil {
				panic(err.Error())
			}
			defer result.Close()
			var url Url
			for result.Next() {
				err := result.Scan(&url.LongURL)
				if err != nil {
					panic(err.Error())
				}

			}
			fmt.Println("Redirected url from mysql.")
			http.Redirect(w, r, url.LongURL, http.StatusSeeOther)
			json.NewEncoder(w).Encode(url)
			Client.Set(Client.Context(), shorturl, url.LongURL, 10*time.Minute).Err()
		} else {
			fmt.Println("Redirected url from redis server.")
			http.Redirect(w, r, LongURL, http.StatusSeeOther)
		}

	}

}

func main() {
	Db()
	db, err = sql.Open("mysql", "root:Qwerty@123@tcp(127.0.0.1:3306)/urls")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	router := mux.NewRouter()
	router.HandleFunc("/api/url", createurl).Methods("POST")
	router.HandleFunc("/{shorturl}", Redirecturl).Methods("GET")

	http.ListenAndServe(":8030", router)

}
