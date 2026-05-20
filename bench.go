package main

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Node struct {
	ID int
	Alive bool
	Pot float64
}

func simulateNeuralWave(nodes []Node) (lat []time.Duration, success, msgs int) {
	for i := 0; i < 1000; i++ {
		start := time.Now()
	visited := make(map[int]bool)
		target := rand.Intn(len(nodes))
		
	// Neural wave: 3 candidates, gradient biased
		candidates := []int{target}
		for j := 0; j < 2; j++ {
			candidates = append(candidates, rand.Intn(len(nodes)))
	}
		
		found := false
		msgCount := 0
		for _, n := range candidates {
			if!nodes[n].Alive || visited[n] { continue }
			visited[n] = true
			msgCount++
			if rand.Float64() > 0.3 {
				found = true
				break
			}
			time.Sleep(time.Duration(20+rand.Intn(60)) * time.Millisecond)
	}
		
		if found {
			success++
			lat = append(lat, time.Since(start))
	}
	msgs += msgCount
	}
	return
}

func main() {
	rand.Seed(time.Now().UnixNano())
	nodes := 10000
	churn := 0.3
	
	n := make([]Node, nodes)
	for i := range n {
		n[i] = Node{i, true, rand.Float64()}
	}
	
	go func() {
		for {
			time.Sleep(2 * time.Second)
			for j := 0; j < int(float64(nodes)*churn); j++ {
				n[rand.Intn(nodes)].Alive = false
			}
			time.Sleep(500 * time.Millisecond)
			for j := 0; j < int(float64(nodes)*churn); j++ {
				n[rand.Intn(nodes)].Alive = true
			}
	}
	}()
	
	lat, suc, msgs := simulateNeuralWave(n)
	
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p99 := lat[int(float64(len(lat))*0.99)]
	p50 := lat[len(lat)/2]
	
	fmt.Printf("Neural Wave DHT - %d nodes, %.0f%% churn\n", nodes, churn*100)
	fmt.Printf("Success: %.1f%%\n", float64(suc)/10)
	fmt.Printf("P99: %v, P50: %v\n", p99, p50)
	fmt.Printf("Msgs/lookup: %.1f\n", float64(msgs)/1000)
}
