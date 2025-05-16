package internal

type MinRecordHeap []Record

func (h MinRecordHeap) Len() int {
	return len(h)
}

func (h MinRecordHeap) Less(i, j int) bool {
	return h[i].Key < h[j].Key
}

func (h MinRecordHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MinRecordHeap) Push(val any) {
	*h = append(*h, val.(Record))
}

func (h *MinRecordHeap) Pop() any {
	heapDereferenced := *h
	size := len(heapDereferenced)
	val := heapDereferenced[size-1]
	*h = heapDereferenced[:size-1]
	return val
}
