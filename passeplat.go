package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"gopkg.in/mcuadros/go-syslog.v2"
	"log"
	"os"
	"strings"
)

var (
	addr    = flag.String("addr", "0.0.0.0:514", "The address to bind to")
	brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	verbose = flag.Bool("verbose", false, "Turn on Sarama logging")
)

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	//server.SetFormat(syslog.RFC5424)
	server.SetFormat(syslog.RFC3164) //BSD format
	server.SetHandler(handler)
	server.ListenUDP(*addr)
	server.Boot()
	dataCollector := newDataCollector(brokerList)

	go func(channel syslog.LogPartsChannel) {
		for logParts := range channel {
			var encoded, err = json.Marshal(logParts)
			fmt.Println(string(encoded))
			if err != nil {
				fmt.Println("Failed to Marshall the syslog data:, %s", err)
			}
			// We are not setting a message key, which means that all messages will
			// be distributed randomly over the different partitions.
			partition, offset, err := dataCollector.SendMessage(&sarama.ProducerMessage{
				Topic: "syslog-passeplat-test",
				Value: sarama.StringEncoder(encoded),
			})

			if err != nil {
				fmt.Println("Failed to store your data:, %s", err)
			} else {
				// The tuple (topic, partition, offset) can be used as a unique identifier
				// for a message in a Kafka cluster.
				fmt.Println("Your data is stored with unique identifier syslog-passeplat-test/%d/%d", partition, offset)
			}
		}
	}(channel)

	server.Wait()
}

func newDataCollector(brokerList []string) sarama.SyncProducer {

	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}
