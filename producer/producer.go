// package main

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log"

// 	"github.com/IBM/sarama"
// 	"github.com/gofiber/fiber/v2"
// )

// type Comment struct {
// 	Text string `form:"text" json:"text`
// }
// type Post struct {
// 	Content string `form:"content" json:"content`
// }

// func main() {
// 	app := fiber.New()
// 	api := app.Group("api/v1")
// 	api.Post("/comments", createComment)
// 	api.Post("/posts", createPost)
// 	app.Listen(":8080")
// }

// func ConnectProducer(brokerUrl []string) (sarama.SyncProducer, error) {
// 	config := sarama.NewConfig()
// 	config.Producer.Return.Successes = true
// 	config.Producer.RequiredAcks = sarama.WaitForAll
// 	config.Producer.Retry.Max = 5
// 	conn, err := sarama.NewSyncProducer(brokerUrl, config)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return conn, err
// }

// func PushCommentToQueue(topic string, message []byte) error {
// 	brokerUrl := []string{"localhost:29092"}
// 	producer, err := ConnectProducer(brokerUrl)
// 	if err != nil {
// 		return err
// 	}
// 	defer producer.Close()
// 	msg := &sarama.ProducerMessage{
// 		Topic: topic,
// 		Value: sarama.StringEncoder(message),
// 	}
// 	partition, offset, err := producer.SendMessage(msg)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
// 	return nil
// }

// func PushPostToQueue(topic string, partition int32, message []byte) error {
// 	brokerUrl := []string{"localhost:29092"}
// 	producer, err := ConnectProducer(brokerUrl)
// 	if err != nil {
// 		return err
// 	}
// 	defer producer.Close()
// 	msg := &sarama.ProducerMessage{
// 		Topic:     topic,
// 		Partition: partition,
// 		Value:     sarama.StringEncoder(message),
// 	}
// 	partition, offset, err := producer.SendMessage(msg)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
// 	return nil
// }

// func createComment(c *fiber.Ctx) error {
// 	cmt := new(Comment)
// 	if err := c.BodyParser(cmt); err != nil {
// 		log.Println(err)
// 		c.Status(400).JSON(&fiber.Map{
// 			"success": false,
// 			"message": err,
// 		})
// 		return err
// 	}
// 	cmtInBytes, err := json.Marshal(cmt)
// 	if err != nil {
// 		return err
// 	}
// 	PushCommentToQueue("comment", cmtInBytes)
// 	err = c.JSON(&fiber.Map{
// 		"success": true,
// 		"message": "Comment Pushed Successsfully",
// 		"comment": "cmt",
// 	})
// 	if err != nil {
// 		c.Status(500).JSON(&fiber.Map{
// 			"success": false,
// 			"message": "Error creating product",
// 		})
// 		return err
// 	}
// 	return err
// }

//	func createPost(c *fiber.Ctx) error {
//		post := new(Post)
//		if err := c.BodyParser(post); err != nil {
//			log.Println("error", err)
//			c.Status(400).JSON(&fiber.Map{
//				"success": false,
//				"message": err,
//			})
//			return err
//		}
//		postInBytes, err := json.Marshal(post)
//		if err != nil {
//			return err
//		}
//		PushPostToQueue("Post", 1, postInBytes)
//		err = c.JSON(&fiber.Map{
//			"success": true,
//			"message": "Comment Pushed Successsfully",
//			"comment": "cmt",
//		})
//		if err != nil {
//			c.Status(500).JSON(&fiber.Map{
//				"success": false,
//				"message": "Error creating product",
//			})
//			return err
//		}
//		return err
//	}
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

type Post struct {
	Content string `form:"content" json:"content"`
}

func main() {
	app := fiber.New()
	api := app.Group("api/v1")
	api.Post("/comments", createComment)
	api.Post("/posts", createPost)
	app.Listen(":8080")
}

func ConnectAdmin(brokerUrl []string) (sarama.ClusterAdmin, error) {
	config := sarama.NewConfig()
	// Create ClusterAdmin to manage topics (like creating partitions)
	admin, err := sarama.NewClusterAdmin(brokerUrl, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster admin: %v", err)
	}
	return admin, err
}

// topic is created by admin
func createTopic(brokerUrl []string, topic string, count int32) error {
	//connect to admin
	admin, err := ConnectAdmin(brokerUrl)
	if err != nil {
		return err
	}
	defer admin.Close() // Close ClusterAdmin when done with administrative tasks

	// Example: Create topic "Post" with 2 partitions (if it doesn't exist)
	detail := &sarama.TopicDetail{
		NumPartitions:     int32(count),
		ReplicationFactor: 1, // Adjust as needed
	}
	err = admin.CreateTopic(topic, detail, false)
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}
	fmt.Printf("Topic '%s' created with %d partitions\n", topic, count)
	return nil
}

// partitions is created by admin
func createPartitions(brokerUrl []string, topic string, count int32) error {
	//connect to admin
	admin, err := ConnectAdmin(brokerUrl)
	if err != nil {
		return err
	}
	defer admin.Close() // Close ClusterAdmin when done with administrative tasks

	// Example: Create topic "Post" with 2 partitions (if it doesn't exist)
	err = admin.CreatePartitions(topic, count, [][]int32{}, false)
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}
	return nil
}

func ConnectProducer(brokerUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	// Now create the SyncProducer for producing messages
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	producer, err := sarama.NewSyncProducer(brokerUrl, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %v", err)
	}

	return producer, nil
}

func PushMessageToQueue(topic string, partition int32, message []byte) error {
	brokerUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokerUrl)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Value:     sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func createComment(c *fiber.Ctx) error {
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
		return err
	}
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		return err
	}

	// Send to partition 0 for comments
	if err := PushMessageToQueue("comment", 0, cmtInBytes); err != nil {
		log.Println(err)
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
		return err
	}

	return c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment Pushed Successfully",
		"comment": cmt,
	})
}

func createPost(c *fiber.Ctx) error {
	post := new(Post)
	if err := c.BodyParser(post); err != nil {
		log.Println("error", err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
		return err
	}
	postInBytes, err := json.Marshal(post)
	if err != nil {
		return err
	}

	// Send to partition 1 for posts
	if err := PushMessageToQueue("Post", 1, postInBytes); err != nil {
		log.Println(err)
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
		return err
	}

	return c.JSON(&fiber.Map{
		"success": true,
		"message": "Post Pushed Successfully",
		"post":    post,
	})
}
