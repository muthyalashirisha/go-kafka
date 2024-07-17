package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func SingleTopic() {
	topic := "comment"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		log.Panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Panic(err)
	}
	fmt.Println("consumer started")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Recieved message Count: %d: | Topic (%s) | Message(%s)\n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigChan:
				fmt.Println("Interruption detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	fmt.Println("Processed ", msgCount, " messages")
	if err := worker.Close(); err != nil {
		log.Panic(err)
	}
}

func DynamicTopics() {
	brokers := []string{"localhost:29092"}
	worker, err := connectConsumer(brokers)
	if err != nil {
		log.Panic(err)
	}

	topics, err := worker.Topics()
	if err != nil {
		log.Panic(err)
	}

	consumers := make([]sarama.PartitionConsumer, 0)
	for _, topic := range topics {
		// Assuming only single partition (0) for simplicity; extend as needed.
		consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
		if err != nil {
			log.Panic(err)
		}
		consumers = append(consumers, consumer)
	}

	fmt.Println("consumer started")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case <-sigChan:
				fmt.Println("Interruption detected")
				doneCh <- struct{}{}
				return
			default:
				for _, consumer := range consumers {
					select {
					case err := <-consumer.Errors():
						if err != nil {
							fmt.Println(err)
						}
					case msg := <-consumer.Messages():
						msgCount++
						fmt.Printf("Received message Count: %d: | Topic (%s) | Message(%s)\n", msgCount, string(msg.Topic), string(msg.Value))
					default:
						// No message received, continue to the next consumer.
					}
				}
			}
		}
	}()
	<-doneCh
	fmt.Println("Processed ", msgCount, " messages")

	// Close consumers
	for _, consumer := range consumers {
		if err := consumer.Close(); err != nil {
			log.Panic(err)
		}
	}

	if err := worker.Close(); err != nil {
		log.Panic(err)
	}
}

// dynamic topics and partions
func main() {
	brokers := []string{"localhost:29092"}
	worker, err := connectConsumer(brokers)
	if err != nil {
		log.Panic(err)
	}

	topics, err := worker.Topics()
	if err != nil {
		log.Panic(err)
	}

	consumers := make([]sarama.PartitionConsumer, 0)
	for _, topic := range topics {
		partitions, err := worker.Partitions(topic)
		if err != nil {
			log.Panic(err)
		}
		fmt.Println(partitions, topic)

		for _, partition := range partitions {
			if partition < 5 { // Consume only from first 2 partitions
				consumer, err := worker.ConsumePartition(topic, partition, sarama.OffsetOldest)
				if err != nil {
					log.Panic(err)
				}
				consumers = append(consumers, consumer)
			}
		}
	}

	fmt.Println("consumer started")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case <-sigChan:
				fmt.Println("Interruption detected")
				doneCh <- struct{}{}
				return
			default:
				for _, consumer := range consumers {
					select {
					case err := <-consumer.Errors():
						if err != nil {
							fmt.Println(err)
						}
					case msg := <-consumer.Messages():
						msgCount++
						fmt.Printf("Received message Count: %d: | Topic (%s) | Partition (%d) | Message(%s)\n", msgCount, string(msg.Topic), msg.Partition, string(msg.Value))
					default:
						// No message received, continue to the next consumer.
					}
				}
			}
		}
	}()
	<-doneCh
	fmt.Println("Processed ", msgCount, " messages")

	// Close consumers
	for _, consumer := range consumers {
		if err := consumer.Close(); err != nil {
			log.Panic(err)
		}
	}

	if err := worker.Close(); err != nil {
		log.Panic(err)
	}
}

// connect to consumer
func connectConsumer(brokerUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokerUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, err
}
