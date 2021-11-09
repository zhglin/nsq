package nsqd

type inFlightPqueue []*Message

// 飞行中的消息优先队列、小顶堆
// 消息的 at least once 机制。
func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

// Swap 元素交换 并交换index值
func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push 添加数据
func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c { // 2倍扩充空间
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	// 添加到尾部
	x.index = n
	(*pq)[n] = x
	// 上浮
	pq.up(n)
}

// Pop 推出堆顶元素，即最小元素
func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	// 把堆顶元素和堆尾元素互换
	pq.Swap(0, n-1)
	// 新的堆顶元素下沉重构小顶堆
	pq.down(0, n-1)
	// 如果新的队列长度小于容量的一半，进行缩容（容量小于25的队列不缩容）
	if n < (c/2) && c > 25 {
		npq := make(inFlightPqueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	// 被置换到堆尾元素的最小元素作为返回值
	x := (*pq)[n-1]
	x.index = -1
	// 队列长度减1
	*pq = (*pq)[0 : n-1]
	return x
}

// Remove 移除i位置的节点
func (pq *inFlightPqueue) Remove(i int) *Message {
	n := len(*pq)
	// 移除的不是最后一个元素
	if n-1 != i {
		pq.Swap(i, n-1) // 交换到最后一个元素
		pq.down(i, n-1) // 进行一次向下调整
		pq.up(i)        // 进行一次向上调整
	}

	// 若要移除的元素是最后一个，则直接移除就好了
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

// PeekAndShift 将入参和堆最小元素比较，小于则返回两者差值，大于则进行pop操作
func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 {
		return nil, 0
	}

	// 取出堆顶元素
	x := (*pq)[0]

	// 若堆顶元素大于max，则return nil，并返回和max的差值
	if x.pri > max {
		return nil, x.pri - max
	}

	// 移除堆顶元素
	pq.Pop()

	// 返回堆顶元素
	return x, 0
}

// 向上堆调整 j需要调整的起始位置
func (pq *inFlightPqueue) up(j int) {
	for {
		// 父节点索引
		i := (j - 1) / 2 // parent
		// 节点关键字大小比较，子更小，则上浮，否则结束循环
		if i == j || (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		// 交换元素
		pq.Swap(i, j)
		j = i
	}
}

// 向下堆调整 i，n需要调整的起始以及终止位置
func (pq *inFlightPqueue) down(i, n int) {
	for {
		// 左子节点索引
		j1 := 2*i + 1
		//  若超过堆的大小或溢出，则直接退出
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		// 把当前节点和自己的两个孩子作比较，若自己的值大于任意孩子的值，把自己和值较小的孩子交换位置。
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri {
			j = j2 // = 2*i + 2  // right child
		}
		if (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		i = j
	}
}
