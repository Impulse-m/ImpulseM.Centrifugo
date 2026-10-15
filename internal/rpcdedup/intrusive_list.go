package rpcdedup

// linkPair are the neighbours of an item in one of the intrusive lists which hold it.
type linkPair[T any] struct {
	prev *T
	next *T
}

// intrusiveList is a doubly linked list of items of type T, whose links are kept in the items, so that pushing and
// removing an item allocates nothing. links returns the links of an item in this list. The store keeps its records
// and its surplus scopes in such lists. TestMemoryAllocsPerCall guards the allocations of a tracked call, which would
// grow if a list allocated per item.
type intrusiveList[T any] struct {
	head  *T
	tail  *T
	len   int
	links func(*T) *linkPair[T]
}

// pushBack appends item to the list.
func (l *intrusiveList[T]) pushBack(item *T) {
	links := l.links(item)
	links.prev = l.tail
	links.next = nil
	if l.tail != nil {
		l.links(l.tail).next = item
	} else {
		l.head = item
	}
	l.tail = item
	l.len++
}

// remove unlinks item, which is in the list.
func (l *intrusiveList[T]) remove(item *T) {
	links := l.links(item)
	if links.prev != nil {
		l.links(links.prev).next = links.next
	} else {
		l.head = links.next
	}
	if links.next != nil {
		l.links(links.next).prev = links.prev
	} else {
		l.tail = links.prev
	}
	links.prev = nil
	links.next = nil
	l.len--
}

// front returns the first item of the list, the oldest one, or nil when it is empty.
func (l *intrusiveList[T]) front() *T {
	return l.head
}
