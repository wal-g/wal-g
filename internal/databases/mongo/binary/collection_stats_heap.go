package binary

const topK int = 100

type CollStats struct {
	NS   string
	Size int64
}

type NsSizeHeap []CollStats

func (h NsSizeHeap) Len() int { return len(h) }
func (h NsSizeHeap) Less(i, j int) bool {
	return h[i].Size < h[j].Size
}
func (h NsSizeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *NsSizeHeap) Push(x any) {
	*h = append(*h, x.(CollStats))
}
func (h *NsSizeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
