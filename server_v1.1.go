package main

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
)

type jsonStatistics struct { //структура для преобразования в json
	Date      string `json:"datetime,omitempty"`
	Ip        string `json:"ipaddr,omitempty"`
	Direction string `json:"windDirection"`
	Speed     string `json:"windSpeed"`
	X         string `json:"x"`
	Y         string `json:"y"`
}

type inclunometer struct { //структура устройства ERD
	id            int
	Type          string
	accuracy      float64
	ip            string
	rs485_address int
	side_id       uint
}

func main() {
	/*получаем все устройства*/
	db, err := sql.Open("mysql", "mtm:GhjcnjqGfhjkm@tcp(localhost:3306)/mtm")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("select id, type, accuracy, ip, rs485_address, side_id from inclunometers")
	if err != nil {
		panic(err)
	}
	/*получаем все устройства*/

	/*преобразуем ответ sql в структуру устройства(стр.27)*/
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
	/*преобразуем ответ sql в структуру устройства(стр.27)*/

	rows.Close()
	db.Close()
	//закрываем соединение с бд

	var wg sync.WaitGroup                   //ждём пока не выполнятся все горутины
	for _, element := range inclunometers { //для каждого устройства ERD
		wg.Add(1)                      //ждём +1 горутину
		go callSurvey(element.ip, &wg) //запуск периодического опроса
	}
	wg.Wait() //ждём горутины
}

func getStatistics(ip string, conn net.Conn) bool {
	/*получаем скорость ветра*/
	_, err := conn.Write([]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0xC4, 0x0B}) //посылаем байты
	if err != nil {
		fmt.Println(err)
	}

	windSpeedStatistics := make([]byte, 10)                  //ёмкость для результата
	_, err = bufio.NewReader(conn).Read(windSpeedStatistics) //читаем ответ и пишем в ёмкость
	if err != nil {
		fmt.Println(err)
	}

	windSpeed := (float64(binary.BigEndian.Uint16(windSpeedStatistics[3:5]) / 100)) //высчитываем результат по формуле
	/*получаем скорость ветра*/

	/*получаем направление ветра*/
	_, err = conn.Write([]byte{0x01, 0x03, 0x00, 0x01, 0x00, 0x02, 0x95, 0xCB}) //посылаем байты
	if err != nil {
		fmt.Println(err)
	}

	windDirection := make([]byte, 10)                  //ёмкость для результата
	_, err = bufio.NewReader(conn).Read(windDirection) //читаем ответ и пишем в ёмкость
	if err != nil {
		fmt.Println(err)
	}

	direction := (float64(binary.BigEndian.Uint16(windDirection[3:5])) / 100) //высчитываем результат по формуле
	/*получаем направление ветра*/

	/*получаем показатели датчика наклона*/
	_, err = conn.Write([]byte{0x68, 0x04, 0x00, 0x04, 0x08}) //посылаем байты
	if err != nil {
		fmt.Println(err)
	}

	inclinimeterStatistics := make([]byte, 14)                  //ёмкость для результата
	_, err = bufio.NewReader(conn).Read(inclinimeterStatistics) //читаем ответ и пишем в ёмкость
	if err != nil {
		fmt.Println(err)
	}

	abscissaX, ordinateY, _ := parseAngle_v2(inclinimeterStatistics[4:10]) //высчитываем результат по формуле
	/*получаем показатели датчика наклона*/

	/*собираем json для записи в redis*/
	Date := time.Now()
	prepareJson := jsonStatistics{Date.Format("2006-01-02 15:04:05"), ip, FloatToString(direction), FloatToString(windSpeed), FloatToString(abscissaX), FloatToString(ordinateY)}
	jsonDecode, err := json.Marshal(prepareJson)
	if err != nil {
		fmt.Println(err)
	}
	/*собираем json для записи в redis*/

	redisConn, err := redis.Dial("tcp", "localhost:6379") //redis соединение
	if err != nil {
		fmt.Println(err)
	}

	redisKey := "ip:" + ip
	currUnix := time.Now().Unix()
	_, err = redisConn.Do("ZADD", redisKey, int(currUnix), jsonDecode) //пишем в redis
	if err != nil {
		fmt.Println(err)
	}

	redisConn.Close()
	return true
}

/*функция для расчёта показателей датчика угла наклона*/
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

/*откорректировать показатели датчика угла наклона*/
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

/*в бесконечном цикле опрашиваеся устройство*/
func callSurvey(ip string, wg *sync.WaitGroup) {
	defer wg.Done()                                //при отработке горутины
	conn, err := net.Dial("tcp", string(ip+":50")) //соединение с устройством
	if err != nil {
		fmt.Println(time.Now(), err)
	}

	for {
		result := getStatistics(ip, conn)

	DELAY:
		time.Sleep(100 * time.Millisecond) //ждём
		if !result {                       //если функция ещё не выполнилась
			goto DELAY //ТО откат
		}
	}
}
