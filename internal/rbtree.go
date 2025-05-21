package internal

// import "github.com/jateen67/kv/utils"

// type color int

// const (
// 	RED color = iota
// 	BLACK
// )

// type Node struct {
// 	Key    string
// 	Value  Record
// 	Parent *Node
// 	Left   *Node
// 	Right  *Node
// 	Color  color
// }

// type RedBlackTree struct {
// 	root *Node
// }

// func (tree *RedBlackTree) Insert(key string, value Record) {
// 	z := &Node{Key: key, Value: value, Color: RED}

// 	var y *Node
// 	x := tree.root
// 	for x != nil {
// 		y = x
// 		if z.Key < x.Key {
// 			x = x.Left
// 		} else {
// 			x = x.Right
// 		}
// 	}

// 	z.Parent = y
// 	if y == nil {
// 		tree.root = z
// 	} else if z.Key < y.Key {
// 		y.Left = z
// 	} else {
// 		y.Right = z
// 	}

// 	z.Left = nil
// 	z.Right = nil
// 	tree.fixInsert(z)
// }

// func (tree *RedBlackTree) fixInsert(z *Node) {
// 	for z.Parent.Color == RED {
// 		if z.Parent == z.Parent.Parent.Left {
// 			y := z.Parent.Parent.Right
// 			if y.Color == RED {
// 				z.Parent.Color = BLACK
// 				y.Color = BLACK
// 				z.Parent.Parent.Color = RED
// 				z = z.Parent.Parent
// 			} else if z == z.Parent.Right {
// 				z = z.Parent
// 				tree.rotateLeft(z)
// 				z.Parent.Color = BLACK
// 				z.Parent.Parent.Color = RED
// 				tree.rotateRight(z.Parent.Parent)
// 			}
// 		} else {
// 			y := z.Parent.Parent.Left
// 			if y.Color == RED {
// 				z.Parent.Color = BLACK
// 				y.Color = BLACK
// 				z.Parent.Parent.Color = RED
// 				z = z.Parent.Parent
// 			} else if z == z.Parent.Left {
// 				z = z.Parent
// 				tree.rotateRight(z)
// 				z.Parent.Color = BLACK
// 				z.Parent.Parent.Color = RED
// 				tree.rotateLeft(z.Parent.Parent)
// 			}
// 		}
// 		if z == tree.root {
// 			break
// 		}
// 	}
// 	tree.root.Color = BLACK
// }

// func (tree *RedBlackTree) rotateRight(x *Node) {
// 	if x == nil || x.Left == nil {
// 		return
// 	}

// 	y := x.Left
// 	x.Left = y.Right
// 	y.Right.Parent = x
// 	y.Parent = x.Parent

// 	if x.Parent == nil {
// 		tree.root = y
// 	} else if x == x.Parent.Right {
// 		x.Parent.Right = y
// 	} else {
// 		x.Parent.Left = y
// 	}
// 	y.Right = x
// 	x.Parent = y
// }

// func (tree *RedBlackTree) rotateLeft(x *Node) {
// 	if x == nil || x.Right == nil {
// 		return
// 	}

// 	y := x.Right
// 	x.Right = y.Left
// 	y.Left.Parent = x
// 	y.Parent = x.Parent

// 	if x.Parent == nil {
// 		tree.root = y
// 	} else if x == x.Parent.Left {
// 		x.Parent.Left = y
// 	} else {
// 		x.Parent.Right = y
// 	}
// 	y.Left = x
// 	x.Parent = y
// }

// func (tree *RedBlackTree) Find(key string) (Record, error) {
// 	curr := tree.root
// 	for curr != nil {
// 		if curr.Key == key {
// 			return curr.Value, nil
// 		} else if key < curr.Key {
// 			curr = curr.Left
// 		} else {
// 			curr = curr.Right
// 		}
// 	}
// 	return Record{}, utils.ErrKeyNotFound
// }

// func (tree *RedBlackTree) ReturnAllRecordsInSortedOrder() []Record {
// 	return inorder(tree.root, []Record{})
// }

// func inorder(node *Node, data []Record) []Record {
// 	curr := node
// 	if curr != nil {
// 		data = inorder(curr.Left, data)
// 		data = append(data, curr.Value)
// 		data = inorder(curr.Right, data)
// 	}
// 	return data
// }
