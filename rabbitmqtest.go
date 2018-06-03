package main

import (
	"fmt"
	"log"
	"bytes"
	"github.com/streadway/amqp"
	"github.com/fatih/stopwatch"
	"sync"
	"time"
	"runtime"
)

var conn *amqp.Connection
var channel *amqp.Channel
var count = 0

const (
	queueName = "asynctest"
	exchange  = "asynctest"
	mqurl ="amqp://shampoo:123456@10.1.62.66:5672"
)

func main() {

	maxProcs := runtime.NumCPU()
	runtime.GOMAXPROCS(maxProcs)

	var err error
	conn, err = amqp.Dial(mqurl)
	failOnErr(err, "failed to connect tp rabbitmq")

	channel, err = conn.Channel()
	failOnErr(err, "failed to open a channel")

	err = channel.ExchangeDeclare(exchange,"topic",false,false,false,false,nil);
	failOnErr(err, "failed to ExchangeDeclare")

	queue,err := channel.QueueDeclare(queueName,true,false,false,false,nil);
	failOnErr(err, "failed to QueueDeclare" + queue.Name)

	channel.QueueBind(queueName,"asynctest",queueName,false,nil);

	channel.Qos(100,0,false);

	forever := make(chan bool)

	go func() {
		s := stopwatch.New()
		s.Start(0)

		var  wg sync.WaitGroup

		for {
			for  k := 0; k < 10000; k = k + 1   {

				wg.Add(1)

				go func() {
					defer wg.Add(-1)
					push()
				}()

			}

			wg.Wait()

			s.Stop()
			fmt.Printf("push Milliseconds elapsed: %v\n", s.ElapsedTime())

			time.Sleep(6*time.Second)

			s.Reset()
			s.Start(0)
		}
	}()

	msgs, err := channel.Consume(queueName, "", false, false, false, false, nil)
	failOnErr(err, "")

	go func() {
		s0 := stopwatch.New()
		s0.Start(0)
		for d := range msgs {
			s := BytesToString(&(d.Body))
			count++
			if count % 10000 == 0 {
				s0.Stop()
				fmt.Printf("receve msg is :%s -- %d Milliseconds elapsed: %v\n", *s, count, s0.ElapsedTime())
				s0.Reset()
				s0.Start(0)
			}
			channel.Ack(d.DeliveryTag,false);
		}
	}()

	<-forever

	close()
}

func failOnErr(err error, msg string) {
	if err != nil {
		log.Fatalf("%s:%s", msg, err)
		panic(fmt.Sprintf("%s:%s", msg, err))
	}
}

func close() {
	channel.Close()
	conn.Close()
}

func push() {

	msgContent := "hello world!"

	channel.Publish(exchange, queueName, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msgContent),
	})
}

func BytesToString(b *[]byte) *string {
	s := bytes.NewBuffer(*b)
	r := s.String()
	return &r
}
