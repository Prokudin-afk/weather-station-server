package main

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
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
	maxSinc       float64
	maxYinc       float64
	maxZinc       float64
	maxVinc       float64
}

func main() {
	/*получаем все устройства*/
	db, err := sql.Open("mysql", "mtm:GhjcnjqGfhjkm@tcp(localhost:3306)/mtm")
	if err != nil {
		logErr(err, "Error opening sql connection")
		return
	}

	inclunometers, err := getErdDevices(db)
	if err != nil {
		return
	}
	/*получаем все устройства*/

	var wg sync.WaitGroup                   //объект, который ждёт выполнения всех горутин
	for _, element := range inclunometers { //для каждого устройства ERD
		wg.Add(1)                       //ждём +1 горутину
		go callSurvey(element, &wg, db) //запуск периодического опроса
	}
	wg.Wait() //ждём горутины
}

func getErdDevices(db *sql.DB) ([]inclunometer, error) { //получаем все устройства ERD
	/*запрос*/
	inclunometers := []inclunometer{}
	rows, err := db.Query("select id, type, accuracy, ip, rs485_address, side_id, maxSinc, maxYinc, maxZinc, maxVinc from inclunometers")
	if err != nil {
		logErr(err, "Error mysql query")
		return inclunometers, err
	}
	/*запрос*/

	/*преобразуем ответ sql в структуру устройства*/
	for rows.Next() {
		p := inclunometer{}
		err := rows.Scan(&p.id, &p.Type, &p.accuracy, &p.ip, &p.rs485_address, &p.side_id, &p.maxSinc, &p.maxYinc, &p.maxZinc, &p.maxVinc)
		if err != nil {
			logErr(err, "Error parsing mysql answer")
			return inclunometers, err
		}
		inclunometers = append(inclunometers, p)
	}
	/*преобразуем ответ sql в структуру устройства*/

	rows.Close()
	return inclunometers, nil
}

func callSurvey(device inclunometer, wg *sync.WaitGroup, db *sql.DB) { //запуск опроса устройств
	defer wg.Done()                               //при отработке горутины
	conn, err := net.Dial("tcp", device.ip+":50") //соединение с устройством
	if err != nil {                               //если не удалось подключиться к устройству
		logErr(err, "Error connecting to ERD")
		setPanic(device, 4, db, "")   //выдаём ошибку
		time.Sleep(5 * time.Second)   //ждём
		go callSurvey(device, wg, db) //и пробуем подключиться снова
		return
	}

	for {
		//получаем скорость ветра
		windSpeed, err := getWindStatistics(conn, []byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0xC4, 0x0B})
		if err != nil {
			logErr(err, "Can`t get wind speed")
			setPanic(device, 3, db, "2")
		}

		//получаем направление ветра
		direction, err := getWindStatistics(conn, []byte{0x01, 0x03, 0x00, 0x01, 0x00, 0x02, 0x95, 0xCB})
		if err != nil {
			logErr(err, "Can`t get wind direction")
			setPanic(device, 3, db, "3")
		}

		//получаем показатели инклинометра
		abscissaX, ordinateY, err := getInclStatistics(conn)
		if err != nil {
			logErr(err, "Can`t get inclinimeter statistics")
			setPanic(device, 3, db, "1")
		}

		//записываем результаты в redis
		err = writeToRedis(device, direction, windSpeed, abscissaX, ordinateY, db)
		if err != nil {
			logErr(err, "Error writing to redis")
		}
	}
}

/*получаем скорость и направление ветра*/
func getWindStatistics(conn net.Conn, comm []byte) (float64, error) {
	_, err := conn.Write(comm)
	if err != nil {
		return 0, err
	}

	windStatistics := make([]byte, 10)
	_, err = bufio.NewReader(conn).Read(windStatistics)
	if err != nil {
		logErr(err, "Can`t parse wind results")
		return 0, err
	}
	stats := float64(binary.BigEndian.Uint16(windStatistics[3:5]) / 100)

	return stats, nil
}

/*получаем показатели датчика наклона*/
func getInclStatistics(conn net.Conn) (float64, float64, error) {
	_, err := conn.Write([]byte{0x68, 0x04, 0x00, 0x04, 0x08})
	if err != nil {
		logErr(err, "Can`t write command getting inc stats")
		return 0, 0, err
	}

	inclinimeterStatistics := make([]byte, 14)
	_, err = bufio.NewReader(conn).Read(inclinimeterStatistics)
	if err != nil {
		logErr(err, "Can`t parse inc stats")
		return 0, 0, err
	}

	abscissaX, ordinateY, err := parseAngle_v2(inclinimeterStatistics[4:10])
	if err != nil {
		logErr(err, "parseAngle_v2")
		return 0, 0, err
	}

	return abscissaX, ordinateY, nil
}

/*собираем json и пишем в redis*/
func writeToRedis(device inclunometer, direction, windSpeed, abscissaX, ordinateY float64, db *sql.DB) error {
	Date := time.Now()
	prepareJson := jsonStatistics{Date.Format("2006-01-02 15:04:05"), device.ip, strconv.FormatFloat(direction, 'f', 6, 64), strconv.FormatFloat(windSpeed, 'f', 6, 64), strconv.FormatFloat(abscissaX, 'f', 6, 64), strconv.FormatFloat(ordinateY, 'f', 6, 64)}
	jsonDecode, err := json.Marshal(prepareJson)
	if err != nil {
		logErr(err, "Trying convert to json")
		return err
	}

	/*Проверка на отклонение показателей инклинометра от нормы*/
	if checkLimit(abscissaX, device.maxSinc, device.maxYinc) != nil {
		setPanic(device, 1, db, string(jsonDecode))
	}

	if checkLimit(ordinateY, device.maxZinc, device.maxVinc) != nil {
		setPanic(device, 2, db, string(jsonDecode))
	}
	/*Проверка на отклонение показателей инклинометра от нормы*/

	/*запись в redis*/
	redisConn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		logErr(err, "Trying open redis connection")
		return err
	}

	redisKey := "ip:" + device.ip
	currUnix := time.Now().Unix()
	_, err = redisConn.Do("ZADD", redisKey, int(currUnix), jsonDecode)
	if err != nil {
		logErr(err, "Error writing to redis")
		return err
	}

	redisConn.Close()
	/*запись в redis*/
	return nil
}

/*функция для расчёта показателей датчика угла наклона*/
func parseAngle_v2(data []byte) (float64, float64, error) {
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
		return floatX, floatY, nil
	} else {
		err := errors.New("incomming data.len is >= 6")
		return 0, 0, err
	}

}

func setRelativeZero(ip string) { //откорректировать показатели датчика угла наклона
	conn, _ := net.Dial("tcp", ip+":50")

	_, err := conn.Write([]byte{0x68, 0x05, 0x00, 0x85, 0x00, 0x8A})
	if err != nil {
		logErr(err, "Error setting inclinometer to zero")
	}
	conn.Close()
}

func logErr(writeErr error, description string) { //логирование в файл
	f, err := os.OpenFile("serverLog.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	logger := log.New(f, description+": ", log.LstdFlags)
	logger.Println(writeErr)
}

func checkLimit(currentVal float64, maxVal float64, minVal float64) error { //проверка на допустимое отклонение
	if (currentVal >= maxVal) || (currentVal <= minVal) {
		return errors.New("показатели инклинометра вне предела допустимых значений")
	}
	return nil
}

func setPanic(incl inclunometer, t int, conn *sql.DB, data string) { //логирование в БД
	id := getLastPanic(incl, t, conn)
	if id == -1 {
		_, err := conn.Exec("INSERT INTO mtm.events (side_id, type, data) VALUES(" + strconv.FormatInt(int64(incl.id), 10) + ", " + strconv.FormatInt(int64(t), 10) + ", '" + data + "')")
		if err != nil {
			logErr(err, "setPanic error")
			os.Exit(1)
		}
	} else {
		updateEvent(id, conn)
	}
}

func getLastPanic(incl inclunometer, t int, conn *sql.DB) int { //поиск записи о конкретной ошибке
	current_time := time.Now()
	last_time := current_time
	cof := -10
	if t == 3 || t == 4 {
		cof = -60
	}

	last_time = last_time.Add(time.Second * time.Duration(cof))
	query := "SELECT id FROM mtm.events where is_read = false AND side_id = " + strconv.FormatInt(int64(incl.id), 10) + " AND type = " + strconv.FormatInt(int64(t), 10) + " AND (time_stop < \"" + current_time.Format("2006-01-02 15:04:05") + "\" AND time_stop > \"" + last_time.Format("2006-01-02 15:04:05") + "\") ORDER BY time_stop DESC LIMIT 1"

	row := conn.QueryRow(query)
	var id int
	err := row.Scan(&id)
	if err != nil {
		id = -1
	}
	return id
}

func updateEvent(last_id int, conn *sql.DB) { //обновление существующей ошибки
	query := "UPDATE mtm.events SET time_stop = NOW() WHERE id = " + strconv.FormatInt(int64(last_id), 10)
	_, err := conn.Exec(query)
	if err != nil {
		logErr(err, "updateEvent error")
		os.Exit(1)
	}
}
