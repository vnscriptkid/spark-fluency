package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

func init() {
	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error while reading config file %s", err)
	}

	viper.AutomaticEnv()
}

type Invoice struct {
	InvoiceID   string  `json:"invoice_id"`
	CustomerID  string  `json:"customer_id"`
	Amount      float64 `json:"amount"`
	InvoiceDate string  `json:"invoice_date"`
	DueDate     string  `json:"due_date"`
}

func produceInvoicesToKafka(invoices []Invoice) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": viper.GetString("BOOTSTRAP_SERVERS"),
		"security.protocol": viper.GetString("SECURITY_PROTOCOL"),
		"sasl.mechanisms":   viper.GetString("SASL_MECHANISMS"),
		"sasl.username":     viper.GetString("SASL_USERNAME"),
		"sasl.password":     viper.GetString("SASL_PASSWORD"),
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	topic := "invoices"
	for _, invoice := range invoices {
		value, err := json.Marshal(invoice)
		if err != nil {
			log.Printf("Failed to marshal invoice: %s", err)
			continue
		}
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
			Key:            []byte(invoice.CustomerID),
		}, nil)
		if err != nil {
			log.Printf("Failed to produce message (err=%s), (invoiceID%s)", err, invoice.InvoiceID)
		} else {
			log.Printf("Produced message (customerID=%s), (invoiceID=%s)", invoice.CustomerID, invoice.InvoiceID)
		}
	}

	// Wait for all messages to be delivered in 15 seconds
	p.Flush(15 * 1000)
	log.Println("All messages produced to Kafka")
}

func main() {
	invoices, err := readInvoicesFromFile("invoices.json")
	if err != nil {
		panic(err)
	}
	produceInvoicesToKafka(invoices)
}

func readInvoicesFromFile(filename string) ([]Invoice, error) {
	// Read invoices from file
	invoices := []Invoice{}
	err := readJSONFile(filename, &invoices)
	if err != nil {
		return nil, err
	}
	return invoices, nil
}

func readJSONFile(filename string, v interface{}) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(v)
	if err != nil {
		return err
	}
	return nil
}
