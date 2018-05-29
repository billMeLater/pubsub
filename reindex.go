package main

import (
	"fmt"
	"os"
	"log"
	"flag"
	"golang.org/x/net/context"
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

func main() {
	projPtr := flag.String("project", "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_PROJECT")
	topicPtr := flag.String("topic", "TOPIC", "TOPIC")
	typePtr := flag.String("type", "delta", "[delta|full]")
	authFilePtr := flag.String("auth", "", "path to auth json file")
	folder := flag.String("folder", "1970-01-01_00-00", "folder name")
	flag.Parse()

	msgType := map[string]bool{
		"full":  true,
		"delta": true,
	}
	if !msgType[*typePtr] {
		fmt.Fprintf(os.Stderr,"\n\t" + *typePtr + " - must be [delta|full]\n")
		os.Exit(1)
	}

	ctx := context.Background()
	// [START auth]
	client, err := pubsub.NewClient(ctx, *projPtr, option.WithCredentialsFile(*authFilePtr))
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}
	// [END auth]

	// Publish a text message on the topic.
	if err := publish(client, *topicPtr, *typePtr, *folder); err != nil {
		log.Fatalf("Failed to publish: %v", err)
	}
}

func publish(client *pubsub.Client, topic, msg, folder string) error {
	ctx := context.Background()
	// [START publish]
	t := client.Topic(topic)
	attr := map[string]string{
		"type": msg,
		"folder": folder,
	}
	result := t.Publish(ctx, &pubsub.Message{Data: []byte(msg + "/" + folder), Attributes: attr})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Published a message; msg ID: %v\n", id)
	// [END publish]
	return nil
}
