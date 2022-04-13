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

	rows, err := db.Query("select id, type, accuracy, ip, rs485_address, side_id from inclunometers")
	if err != nil {
		panic(err)
	}

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

	rows.Close()
	db.Close()

	for _, element := range inclunometers {
		conn, err := net.Dial("tcp", string(element.ip+":50"))
		if err != nil {
			fmt.Println(time.Now(), err)
		}
		//setRelativeZero(element.ip)
		for {
			start := time.Now()
			result := getStatistics(element.ip, conn)
		DELAY:
			time.Sleep(100 * time.Millisecond)
			if !result {
				goto DELAY
			}
			fmt.Println(time.Since(start))
		}
	}
}

func getStatistics(ip string, conn net.Conn) bool {
	_, err := conn.Write([]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0xC4, 0x0B})
	if err != nil {
		fmt.Println(err)
	}

	windSpeedStatistics := make([]byte, 10)
	_, err = bufio.NewReader(conn).Read(windSpeedStatistics)
	if err != nil {
		fmt.Println(err)
	}
	windSpeed := (float64(binary.BigEndian.Uint16(windSpeedStatistics[3:5]) / 100))

	_, err = conn.Write([]byte{0x01, 0x03, 0x00, 0x01, 0x00, 0x02, 0x95, 0xCB})
	if err != nil {
		fmt.Println(err)
	}

	windDirection := make([]byte, 10)
	_, err = bufio.NewReader(conn).Read(windDirection)
	if err != nil {
		fmt.Println(err)
	}
	direction := (float64(binary.BigEndian.Uint16(windDirection[3:5])) / 100)

	_, err = conn.Write([]byte{0x68, 0x04, 0x00, 0x04, 0x08})
	if err != nil {
		fmt.Println(err)
	}

	inclinimeterStatistics := make([]byte, 14)
	_, err = bufio.NewReader(conn).Read(inclinimeterStatistics)
	if err != nil {
		fmt.Println(err)
	}
	abscissaX, ordinateY, _ := parseAngle_v2(inclinimeterStatistics[4:10])

	return writeToRedis(ip, direction, windSpeed, abscissaX, ordinateY)
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

func writeToRedis(ip string, direction float64, windSpeed float64, abscissaX float64, ordinateY float64) bool {
	Date := time.Now()
	prepareJson := jsonStatistics{Date.Format("2006-01-02 15:04:05"), ip, FloatToString(direction), FloatToString(windSpeed), FloatToString(abscissaX), FloatToString(ordinateY)}
	jsonDecode, err := json.Marshal(prepareJson)
	if err != nil {
		fmt.Println(err)
	}

	redisKey := "ip:" + ip
	currUnix := time.Now().Unix()
	redisConn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		fmt.Println(err)
	}

	_, err = redisConn.Do("ZADD", redisKey, int(currUnix), jsonDecode)
	if err != nil {
		fmt.Println(err)
	}

	redisConn.Close()
	return true
}
