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

type ClusterClient struct {
	client  *redis.ClusterClient
	context context.Context
}

func NewClusterClient(ctx context.Context, properties *Property) (Client, error) {
	if properties.Cluster == nil {
		return nil, errors.New("未提供集群节点。")
	}

	cluster := properties.Cluster
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:              cluster.Nodes,
		Password:           properties.Password,
		PoolSize:           cluster.PoolSize,
		MinIdleConns:       cluster.MinIdleConns,
		MaxRedirects:       cluster.MaxRedirects,
		ReadOnly:           cluster.ReadOnly,
		RouteByLatency:     cluster.RouteByLatency,
		RouteRandomly:      cluster.RouteRandomly,
		MaxRetries:         cluster.MaxRetries,
		MinRetryBackoff:    cluster.MinRetryBackoff,
		MaxRetryBackoff:    cluster.MaxRetryBackoff,
		DialTimeout:        cluster.DialTimeout,
		ReadTimeout:        cluster.ReadTimeout,
		WriteTimeout:       cluster.WriteTimeout,
		PoolTimeout:        cluster.PoolTimeout,
		IdleCheckFrequency: cluster.IdleCheckFrequency,
		IdleTimeout:        cluster.IdleTimeout,
		MaxConnAge:         cluster.MaxConnAge,
	})
	// 检查各个节点的网络状况
	err := client.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	})
	if err != nil {
		log4g.Error("%+v", err)
	}
	return &ClusterClient{client, ctx}, nil
}

func (t *ClusterClient) Count(format string) map[string]uint64 {
	format = strings.TrimSpace(format)
	matchedTotalForEachNode := make(map[string]uint64)
	// 遍历集群中的主节点
	err := t.client.ForEachMaster(t.context, func(ctx context.Context, rdb *redis.Client) error {
		nodeAddress := rdb.Options().Addr
		log4g.Info("开始扫描[%s]...", nodeAddress)
		defer func() {
			log4g.Info("结束扫描[%s]！", nodeAddress)
		}()

		if format == "" {
			// 若未指定format，则统计节点的所有键值对数量
			total, err := t.client.DBSize(ctx).Result()
			if err != nil {
				log4g.Error("节点[%s]统计异常：%+v", nodeAddress, err)
				matchedTotalForEachNode[nodeAddress] = 0
			} else {
				matchedTotalForEachNode[nodeAddress] = uint64(total)
			}
		} else {
			matchedTotalForEachNode[nodeAddress] = t.scanForCounting(ctx, rdb, format)
		}
		return nil
	})
	if err != nil {
		log4g.Error("%+v", err)
	}

	return matchedTotalForEachNode
}

func (t *ClusterClient) Clear(format string) map[string]uint64 {
	format = strings.TrimSpace(format)
	var keyChannel = make(chan string, 1000)
	var matchedTotalForEachNode = make(map[string]uint64)

	// 为提高删除效率，使用goroutine和channel实现查删同步。
	var wg = sync.WaitGroup{}
	wg.Add(2)
	// 查找协程
	go func() {
		err := t.client.ForEachMaster(t.context, func(ctx context.Context, rdb *redis.Client) error {
			shardAddr := rdb.Options().Addr
			log4g.Info("开始扫描[%s]...", shardAddr)
			defer func() {
				log4g.Info("结束扫描[%s]！", shardAddr)
			}()

			matchedTotalForEachNode[shardAddr] = t.scanForDeleting(ctx, rdb, format, keyChannel)
			return nil
		})
		close(keyChannel)

		if err != nil {
			log4g.Error("%+v", err)
		}
		wg.Done()
	}()

	// 删除协程
	var delTotal int64
	go func() {
		delTotal = t.delBatch(keyChannel)
		wg.Done()
	}()
	wg.Wait()

	return matchedTotalForEachNode
}

func (t *ClusterClient) Close() {
	if t.client != nil {
		if err := t.client.Close(); err != nil {
			log4g.Error("Redis客户端关闭失败")
		}
	}
}

func (t *ClusterClient) scanForDeleting(ctx context.Context, rdb *redis.Client, keyFormat string, keyChannel chan<- string) uint64 {
	var err error
	var hasNext = true
	var cursor, matchedTotal uint64
	for hasNext {
		var keys []string
		if keys, cursor, err = rdb.Scan(ctx, cursor, keyFormat, 0).Result(); err != nil {
			log4g.Error("节点[%s]扫描异常：%+v", rdb.Options().Addr, err)
			return 0
		}

		for _, key := range keys {
			keyChannel <- key
		}
		matchedTotal += uint64(len(keys))

		hasNext = cursor != 0
	}
	return matchedTotal
}

func (t *ClusterClient) delBatch(keyChannel <-chan string) int64 {
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

func (t *ClusterClient) flush(pipeline *redis.Pipeliner, commands []*redis.IntCmd) (int64, error) {
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

func (t *ClusterClient) scanForCounting(ctx context.Context, rdb *redis.Client, keyFormat string) uint64 {
	var err error
	var hasNext = true
	var cursor, matchedTotal uint64
	for hasNext {
		var keys []string
		// 扫描当前节点中匹配的键，并返回。
		// @param ctx    上下文。
		// @param cursor 游标。从0开始，每次调用会返回新的游标，若新游标为0，则迭代结束。
		// @param match  匹配的键名格式
		// @param count  最多返回的匹配项个数，小于等于0时取默认值。默认：10，
		if keys, cursor, err = rdb.Scan(ctx, cursor, keyFormat, 0).Result(); err != nil {
			log4g.Error("节点[%s]扫描异常：%+v", rdb.Options().Addr, err)
			return 0
		}
		matchedTotal = matchedTotal + uint64(len(keys))

		hasNext = cursor != 0
	}
	return matchedTotal
}
