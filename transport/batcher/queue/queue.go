package queue

import "github.com/Nextdoor/pg-bifrost.git/transport"

// An Item is something we manage in a priority queue.
type BatchQueueItem struct {
	Batch    transport.Batch // The value of the item; arbitrary.
	Priority int64           // The priority of the item in the queue.
	Index    int             // The index of the item in the heap.
	Key      string          // Batching key for the batch
}

type BatchQueue []*BatchQueueItem

// Mostly copy paste from https://golang.org/pkg/container/heap/

func (bq BatchQueue) Len() int { return len(bq) }

func (bq BatchQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return bq[i].Priority > bq[j].Priority
}

func (bq BatchQueue) Swap(i, j int) {
	bq[i], bq[j] = bq[j], bq[i]
	bq[i].Index = i
	bq[j].Index = j
}

func (pq *BatchQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*BatchQueueItem)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *BatchQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
