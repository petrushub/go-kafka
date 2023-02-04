package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Verificar si se proporcionaron todos los argumentos necesarios
	if len(os.Args) < 5 {
		fmt.Println("Uso: go run main.go <broker_server> <username> <password> <topic>")
		os.Exit(1)
	}

	// Asignar los argumentos a las variables correspondientes
	brokerServer := os.Args[1]
	username := os.Args[2]
	password := os.Args[3]
	topic := os.Args[4]

	// Crear una nueva instancia de Consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerServer,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
		"sasl.username":     username,
		"sasl.password":     password,
	})

	if err != nil {
		panic(err)
	}

	defer c.Close()

	// Suscribirse al tópico
	c.SubscribeTopics([]string{topic}, nil)

	// Contador de mensajes
	messageCount := 0

	// Tamaño total del tópico
	topicSize := 0

	// Bucle infinito para consumir mensajes
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			// Aumentar el contador de mensajes
			messageCount++
			// Aumentar el tamaño total del tópico
			topicSize += len(msg.Value)
		} else {
			// Salir del bucle cuando se haya alcanzado el fin de los mensajes
			break
		}
	}

	// Imprimir el número de mensajes y el tamaño total del tópico
	fmt.Printf("Número de mensajes en el tópico %s: %d\n", topic, messageCount)
	fmt.Printf("Tamaño total del tópico %s: %d bytes\n", topic, topicSize)
}
