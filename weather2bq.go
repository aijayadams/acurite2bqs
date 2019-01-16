package main

import (
	"bufio"
	"cloud.google.com/go/bigquery"
	"encoding/json"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"log"
	"os"
	"time"
)

// Item represents a row item.
type weatherProbe struct {
	Time        time.Time
	Temperature float32
	WindSpeed   float32
	Humidity    float32
}

type inputJsonHolder struct {
	Time         string  `json:"time"`
	Model        string  `json:"model"`
	SensorId     int64   `json:"sensor_id"`
	Channel      string  `json:"channel"`
	SequenceNum  int64   `json:"sequence_num"`
	Battery      string  `json:"battery"`
	MessageType  int64   `json:"message_type"`
	WindSpeedMPH float32 `json:"wind_speed_mph"`
	TemperatureF float32 `json:"temperature_F"`
	Humidity     int64   `json:"humidity"`
}

// ValueSaver interface
func (i *weatherProbe) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"Time":        i.Time,
		"Temperature": i.Temperature,
		"WindSpeed":   i.WindSpeed,
		"Humidity":    i.Humidity,
	}, "", nil
}

func fahrenheitToCelsius(tempF float32) float32 {
	return ((tempF - 32.0) * 5.0 / 9.0)
}

func milesTokilometers(miles float32) float32 {
	return miles * 1.60934
}

func sendWeatherToBQ(messages chan inputJsonHolder, uploader *bigquery.Uploader) {
	ctx := context.Background()
	for {
		// Try get data from the channel
		data := <-messages

		now := time.Now()
		// Marshel data for BQ
		items := []*weatherProbe{
			{Time: now, Temperature: fahrenheitToCelsius(data.TemperatureF), WindSpeed: milesTokilometers(data.WindSpeedMPH), Humidity: float32(data.Humidity)},
		}

		if err := uploader.Put(ctx, items); err != nil {
			continue
		}

		fmt.Print("Temperature: ", fahrenheitToCelsius(data.TemperatureF), "C")
		fmt.Println(" Wind Speed:  ", milesTokilometers(data.WindSpeedMPH), "Kph")
	}
}

func main() {

	// Read user supplied flags
	var kSubmitInterval = flag.Int("interval", 30, "Interval to submit weather data to BigQuery")
	flag.Parse()

	submitInterval := time.Duration(*kSubmitInterval) * time.Second

	// Connect to GCP BigQuery
	bq_acct := "homeweather-225222"
	bq_dataset := "weather"
	bq_table := "weatherProg"

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, bq_acct)
	if err != nil {
		fmt.Println(err)
	}

	uploader := client.Dataset(bq_dataset).Table(bq_table).Uploader()

	// Start routine that will upload data
	messages := make(chan inputJsonHolder)
	go sendWeatherToBQ(messages, uploader)

	// Handle JSON input
	// {"time" : "2018-12-22 16:31:14", "model" : "Acurite 3n1 sensor", "sensor_id" : 2618, "channel" : "A", "sequence_num" : 3, "battery" : "LOW", "message_type" : 32, "wind_speed_mph" : 2.654, "temperature_F" : 51.000, "humidity" : 70}
	inputJson := new(inputJsonHolder)
	sendTime := time.Now().Add(-submitInterval)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		err := json.Unmarshal([]byte(line), &inputJson)
		if err != nil {
			// Unable to parse input
			log.Println(err)
		} else {
			// Make sure it's been at least submitInterval since last
			// time we sent data
			if time.Now().After(sendTime.Add(submitInterval)) {
				sendTime = time.Now()
				// Send data to sendWeatherToBQ
				messages <- *inputJson
			}
		}
	}

	return
}
