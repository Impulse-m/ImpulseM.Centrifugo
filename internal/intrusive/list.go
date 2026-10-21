// Package intrusive provides doubly linked lists whose links are kept in the items, so that pushing and removing an item
// allocates nothing.
package intrusive

// Links are the neighbours of an item in one of the lists which hold it. A zero Links is in no list.
type Links[T any] struct {
	prev *T
	next *T
}

// Next returns the next item of the list, nil at the back of the list or when the item is in no list.
func (l *Links[T]) Next() *T {
	return l.next
}

// List is a doubly linked list of items of type T, whose links are kept in the items. The links function returns the
// links of an item in this list. The zero List is not usable: create it with NewList.
type List[T any] struct {
	head  *T
	tail  *T
	len   int
	links func(*T) *Links[T]
}

// NewList creates an empty List whose items are linked by links.
func NewList[T any](links func(*T) *Links[T]) List[T] {
	return List[T]{links: links}
}

// PushBack appends item to the list.
func (l *List[T]) PushBack(item *T) {
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

// Remove unlinks item, which is in the list.
func (l *List[T]) Remove(item *T) {
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

// Front returns the first item of the list, the oldest one, or nil when it is empty.
func (l *List[T]) Front() *T {
	return l.head
}

// Len returns the number of items in the list.
func (l *List[T]) Len() int {
	return l.len
}
