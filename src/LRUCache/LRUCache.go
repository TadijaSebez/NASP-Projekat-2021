package LRUCache
import (
	"container/list"
)

type cacheElement struct {
	key   string
	value []byte
}
type LRUCache struct{
	capacity int
	queue  *list.List
	keyMap map[string]*list.Element
}
func Constructor(capacity int) LRUCache{
	c := LRUCache{}
	c.capacity = capacity
	c.queue = new(list.List)
	c.keyMap = make(map[string]*list.Element, capacity)
	return c
}
func (c *LRUCache) Get(key string) (bool, []byte){
	if elem, ok := c.keyMap[key]; ok{
		c.queue.MoveToFront(elem)
		return true, elem.Value.(*list.Element).Value.(cacheElement).value
	}
	return false, nil
}
func (c *LRUCache) Put (key string, value []byte){
	if elem, ok := c.keyMap[key]; ok{
		c.queue.MoveToFront(elem)
		elem.Value.(*list.Element).Value = cacheElement{
			key: key,
			value: value}
	}else{
		if c.queue.Len() >= c.capacity{
			last := c.queue.Back().Value.(*list.Element).Value.(cacheElement).key
			delete(c.keyMap, last)
			c.queue.Remove(c.queue.Back())
		}
		elem := &list.Element{
			Value: cacheElement{
				key: key,
				value: value,
			},
		}
		newElem := c.queue.PushFront(elem)
		c.keyMap[key] = newElem
	}
}