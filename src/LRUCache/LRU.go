package LRUCache
import (
	"container/list"
)

type LRUCache struct{
	capacity int
	queue  *list.List
	keyMap map[string]*list.Element
}
type Pair struct {
	key   string
	value []byte
}

func Constructor(capacity int) LRUCache{
	c := LRUCache{}
	c.capacity = capacity
	c.queue = new(list.List)
	c.keyMap = make(map[string]*list.Element, capacity)
	return c
}
func (c *LRUCache) Get(key string) []byte{
	if elem, ok := c.keyMap[key]; ok{
		c.queue.MoveToFront(elem)
		return elem.Value.(*list.Element).Value.(Pair).value
	}
	return nil
}
func (c *LRUCache) Put (key string, value []byte){
	if elem, ok := c.keyMap[key]; ok{
		c.queue.MoveToFront(elem)
		elem.Value.(*list.Element).Value = Pair{key: key, value: value}
	}else{
		if c.queue.Len() == c.capacity{
			index := c.queue.Back().Value.(*list.Element).Value.(Pair).key
			delete(c.keyMap, index)
			c.queue.Remove(c.queue.Back())
		}
		elem := &list.Element{
			Value: Pair{
				key: key,
				value: value,
			},
		}
		pointer := c.queue.PushFront(elem)
		c.keyMap[key] = pointer
	}
}