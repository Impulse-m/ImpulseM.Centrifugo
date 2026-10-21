package intrusive

import (
	"container/list"
	"testing"

	"github.com/stretchr/testify/require"
)

type testItem struct {
	name  string
	links Links[testItem]
}

func testLinks(item *testItem) *Links[testItem] {
	return &item.links
}

// names returns the names of the items of the list, from the front.
func names(l *List[testItem]) []string {
	var result []string
	for item := l.Front(); item != nil; item = item.links.Next() {
		result = append(result, item.name)
	}
	return result
}

func TestListPushRemoveFront(t *testing.T) {
	l := NewList(testLinks)
	a := &testItem{name: "a"}
	b := &testItem{name: "b"}
	c := &testItem{name: "c"}
	l.PushBack(a)
	l.PushBack(b)
	l.PushBack(c)
	require.Equal(t, []string{"a", "b", "c"}, names(&l))
	require.Equal(t, 3, l.Len())

	l.Remove(b)
	require.Equal(t, []string{"a", "c"}, names(&l))
	require.Equal(t, 2, l.Len())
	require.Nil(t, b.links.prev)
	require.Nil(t, b.links.next)

	l.Remove(a)
	require.Same(t, c, l.Front())

	l.Remove(c)
	require.Nil(t, l.Front())
	require.Equal(t, 0, l.Len())
	require.Nil(t, c.links.Next())
}

// BenchmarkListRemovePushBack moves one of 1,024 items to the back of its list per op. The intrusive list allocates
// nothing, while container/list allocates one element per push.
func BenchmarkListRemovePushBack(b *testing.B) {
	const count int = 1024
	b.Run("intrusive", func(b *testing.B) {
		l := NewList(testLinks)
		items := make([]*testItem, count)
		for i := range items {
			items[i] = &testItem{}
			l.PushBack(items[i])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			item := items[i%count]
			l.Remove(item)
			l.PushBack(item)
		}
	})
	b.Run("container-list", func(b *testing.B) {
		l := list.New()
		elems := make([]*list.Element, count)
		for i := range elems {
			elems[i] = l.PushBack(&testItem{})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			j := i % count
			value := l.Remove(elems[j])
			elems[j] = l.PushBack(value)
		}
	})
}
