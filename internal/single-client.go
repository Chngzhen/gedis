package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Chngzhen/log4g"
	"github.com/go-redis/redis/v8"
)

type SingleClient struct {
	client  *redis.Client
	context context.Context
}

func NewSingleClient(ctx context.Context, properties *Property) (Client, error) {
	// 创建单例客户端实例
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", properties.Host, properties.Port),
		Password: properties.Password,
		DB:       properties.Database,
	})
	// 检查Redis的网络状况
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}
	return &SingleClient{client, ctx}, nil
}

func (t *SingleClient) Count(format string) map[string]uint64 {
	format = strings.TrimSpace(format)

	var err error
	var hasNext = true
	var cursor, matchedTotal uint64
	for hasNext {
		var keys []string
		if keys, cursor, err = t.client.Scan(t.context, cursor, format, 0).Result(); err != nil {
			log4g.Error("扫描异常：%+v", err)
			return nil
		} else {
			matchedTotal += uint64(len(keys))
		}
		hasNext = cursor != 0
	}

	address := t.client.Options().Addr
	matchedTotalForEachNode := make(map[string]uint64)
	matchedTotalForEachNode[address] = matchedTotal
	return matchedTotalForEachNode
}

func (t *SingleClient) Clear(format string) map[string]uint64 {
	format = strings.TrimSpace(format)
	if format == "" {
		log4g.Error("匹配格式不能为空")
		return nil
	}
	address := t.client.Options().Addr

	var keyChannel = make(chan string, 1000)
	var matchedTotalForEachNode = make(map[string]uint64)

	// 为提高删除效率，使用goroutine和channel实现查删同步。
	var wg = sync.WaitGroup{}
	wg.Add(2)
	// 查找协程
	go func() {
		defer wg.Done()

		var err error
		var hasNext = true
		var cursor, matchedTotal uint64
		for hasNext {
			var keys []string
			if keys, cursor, err = t.client.Scan(t.context, cursor, format, 0).Result(); err != nil {
				log4g.Error("扫描异常：%+v", err)
				return
			} else {
				for _, key := range keys {
					keyChannel <- key
				}
				matchedTotal += uint64(len(keys))
			}
			hasNext = cursor != 0
		}
		matchedTotalForEachNode[address] = matchedTotal
		close(keyChannel)
	}()

	// 删除协程
	var delTotal int64
	go func() {
		defer wg.Done()
		delTotal = t.delBatch(keyChannel)
	}()
	wg.Wait()

	return matchedTotalForEachNode
}

func (t *SingleClient) Close() {
	if t.client != nil {
		if err := t.client.Close(); err != nil {
			log4g.Error("Redis客户端关闭失败")
		}
	}
}

func (t *SingleClient) delBatch(keyChannel <-chan string) int64 {
	var n int
	var delTotal int64
	commands := make([]*redis.IntCmd, 500)

	pipeline := t.client.Pipeline()
	for key := range keyChannel {
		index := n % 500
		commands[index] = pipeline.Del(t.context, key)

		n++
		if n%500 == 0 {
			if num, err := t.flush(&pipeline, commands); err != nil {
				log4g.Error("管道提交失败：%+v", err)
			} else {
				delTotal += num
			}
		}
	}
	if n%500 != 0 {
		if num, err := t.flush(&pipeline, commands); err != nil {
			log4g.Error("管道提交失败：%+v", err)
		} else {
			delTotal += num
		}
	}
	return delTotal
}

func (t *SingleClient) flush(pipeline *redis.Pipeliner, commands []*redis.IntCmd) (int64, error) {
	if _, err := (*pipeline).Exec(t.context); err != nil {
		log4g.Error(fmt.Sprintf("%+v", err))
		return 0, errors.New(fmt.Sprintf("批量删除失败"))
	} else {
		var deletedNum int64
		for i, cmd := range commands {
			if cmd != nil {
				deletedNum += cmd.Val()
				commands[i] = nil
			}
		}
		return deletedNum, nil
	}
}
