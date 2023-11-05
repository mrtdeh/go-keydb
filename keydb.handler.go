package keydb

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

type Client struct {
	conn rueidis.Client
}

var client *Client

var ctx = context.Background()

type PubData []byte

var pub sync.RWMutex

type Config struct {
	Host      string
	Port      int
	Pass      string
	DB        int
	TLSConfig *tls.Config
}

type KeyVal struct {
	Key string
	Val string
	Ex  time.Duration
}

func Init(opt Config) error {
	pub.Lock()
	defer pub.Unlock()

	if client != nil {
		return nil
	}

	redisopt := rueidis.ClientOption{
		InitAddress: []string{fmt.Sprintf("%s:%d", opt.Host, opt.Port)},
		Password:    opt.Pass,
		TLSConfig:   opt.TLSConfig,
		SelectDB:    opt.DB,
	}

	rueidisClient, err := rueidis.NewClient(redisopt)
	if err != nil {
		return fmt.Errorf("error in connecting to redis: %s", err.Error())
	}

	res := rueidisClient.Do(ctx, rueidisClient.B().Ping().Build())
	if res.Error() != nil {
		return fmt.Errorf("redis ping failed : %s", res.Error().Error())
	}

	client = &Client{
		conn: rueidisClient,
	}

	log.Println("successfuly connect to redis server")
	return nil
}

func Set(kv KeyVal) error {
	cmd := client.conn.B().Set().Key(kv.Key).Value(kv.Val)
	if kv.Ex != 0 {
		cmd.Ex(kv.Ex)
	}
	resp := client.conn.Do(ctx, cmd.Build())
	if err := resp.Error(); err != nil {
		return err
	}
	return nil
}
func Get(key string) (*rueidis.RedisResult, error) {
	cmd := client.conn.B().Get().Key(key).Build()
	resp := client.conn.Do(ctx, cmd)
	if err := resp.Error(); err != nil {
		return nil, err
	}
	return &resp, nil
}

func Scan(cursor uint64, pattern string, count int64) (*rueidis.RedisResult, error) {
	cmd := client.conn.B().Scan().Cursor(cursor).Match(pattern).Count(count).Build()
	resp := client.conn.Do(ctx, cmd)
	if err := resp.Error(); err != nil {
		return nil, err
	}
	return &resp, nil
}

func SetMulti(kvs []KeyVal) error {
	pub.Lock()
	defer pub.Unlock()

	var cmds = make(rueidis.Commands, 0, len(kvs))
	for _, m := range kvs {
		cmd := client.conn.B().Set().Key(m.Key).Value(m.Val)
		if m.Ex != 0 {
			cmd.Ex(m.Ex)
		}
		cmds = append(cmds, cmd.Build())
	}
	for _, resp := range client.conn.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

func Publish(chanel string, msgs []PubData) error {
	pub.Lock()
	defer pub.Unlock()

	var cmds = make(rueidis.Commands, 0, len(msgs))

	for _, m := range msgs {
		cmds = append(cmds, client.conn.B().Publish().Channel(chanel).Message(string(m)).Build().Pin())
	}

	for _, resp := range client.conn.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func Subscribe(msgChan chan interface{}, chanel ...string) {

	cmd := client.conn.B().Subscribe().Channel(chanel...).Build()
	go func() {
		err := client.conn.Receive(ctx, cmd, func(msg rueidis.PubSubMessage) {
			// msgCh <- msg.Message
			msgChan <- msg.Message
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

}
