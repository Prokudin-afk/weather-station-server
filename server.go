package main

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
)

type jsonStatistics struct {
	Date      string `json:"datetime,omitempty"`
	Ip        string `json:"ipaddr,omitempty"`
	Direction string `json:"windDirection"`
	Speed     string `json:"windSpeed"`
	X         string `json:"x"`
	Y         string `json:"y"`
}

type inclunometer struct {
	id            int
	Type          string
	accuracy      float64
	ip            string
	rs485_address int
	side_id       uint
}

func main() {
	db, err := sql.Open("mysql", "mtm:GhjcnjqGfhjkm@tcp(localhost:3306)/mtm")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query("select id, type, accuracy, ip, rs485_address, side_id from inclunometers")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	inclunometers := []inclunometer{}
	for rows.Next() {
		p := inclunometer{}
		err := rows.Scan(&p.id, &p.Type, &p.accuracy, &p.ip, &p.rs485_address, &p.side_id)
		if err != nil {
			fmt.Println(err)
			continue
		}
		inclunometers = append(inclunometers, p)
	}

	for _, element := range inclunometers {
		//setRelativeZero(element.ip)
		for {
			go getStatistics(element.ip)
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func getStatistics(ip string) (float64, float64, float64, float64) {
	conn, _ := net.Dial("tcp", string(ip+":50"))

	_, err := conn.Write([]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0xC4, 0x0B})
	if err != nil {
		panic(err)
	}

	windSpeedStatistics := make([]byte, 10)
	message, _ := bufio.NewReader(conn).Read(windSpeedStatistics)
	if message == 0 {
		panic("can`t get wind stats")
	}
	windSpeed := (float64(binary.BigEndian.Uint16(windSpeedStatistics[3:5]) / 100))

	_, err = conn.Write([]byte{0x01, 0x03, 0x00, 0x01, 0x00, 0x02, 0x95, 0xCB})
	if err != nil {
		panic(err)
	}

	windDirection := make([]byte, 10)
	message, _ = bufio.NewReader(conn).Read(windDirection)
	if message == 0 {
		panic("can`t get wind stats")
	}
	direction := (float64(binary.BigEndian.Uint16(windDirection[3:5])) / 100)

	_, err = conn.Write([]byte{0x68, 0x04, 0x00, 0x04, 0x08})
	if err != nil {
		panic(err)
	}

	inclinimeterStatistics := make([]byte, 14)
	message, _ = bufio.NewReader(conn).Read(inclinimeterStatistics)
	if message == 0 {
		panic("can`t get incl stats")
	}
	abscissaX, ordinateY, _ := parseAngle_v2(inclinimeterStatistics[4:10])
	Date := time.Now()

	prepareJson := jsonStatistics{Date.Format("2006-01-02 15:04:05"), ip, FloatToString(direction), FloatToString(windSpeed), FloatToString(abscissaX), FloatToString(ordinateY)}
	jsonDecode, err := json.Marshal(prepareJson)
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	/*REDIS*/
	redisKey := "ip:" + ip
	currUnix := time.Now().Unix()
	redisConn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}

	v, err := redisConn.Do("ZADD", redisKey, int(currUnix), jsonDecode)
	if v == 0 || err != nil {
		panic(err)
	}
	defer redisConn.Close()
	/*REDIS*/

	return windSpeed, direction, abscissaX, ordinateY
}

func parseAngle_v2(data []byte) (float64, float64, int) {
	if len(data) >= 6 {
		rawX := data[0:3]
		rawY := data[3:6]
		tmp1, _ := strconv.ParseFloat(strconv.FormatInt(int64(rawX[1]), 16), 16)
		tmp2, _ := strconv.ParseFloat(strconv.FormatInt(int64(rawX[2]), 16), 16)
		floatX := tmp1 + (tmp2 / 100)
		tmp1, _ = strconv.ParseFloat(strconv.FormatInt(int64(rawY[1]), 16), 16)
		tmp2, _ = strconv.ParseFloat(strconv.FormatInt(int64(rawY[2]), 16), 16)
		floatY := tmp1 + (tmp2 / 100)
		if rawX[0] == 16 {
			floatX = floatX * -1
		}
		if rawY[0] == 16 {
			floatY = floatY * -1
		}
		return floatX, floatY, 0
	} else {
		return 0, 0, 1
	}

}

func setRelativeZero(ip string) {
	conn, _ := net.Dial("tcp", string(ip+":50"))

	_, err := conn.Write([]byte{0x68, 0x05, 0x00, 0x85, 0x00, 0x8A})
	if err != nil {
		panic(err)
	}
	conn.Close()
}

func FloatToString(input_num float64) string {
	return strconv.FormatFloat(input_num, 'f', 6, 64)
}
