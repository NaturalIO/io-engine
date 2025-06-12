use std::{fmt, mem::transmute, ptr::null_mut};

pub struct EmbeddedListNode {
    pub prev: *mut EmbeddedListNode,
    pub next: *mut EmbeddedListNode,
    l: *mut EmbeddedList,
}

unsafe impl Sync for EmbeddedListNode {}
unsafe impl Send for EmbeddedListNode {}

impl Default for EmbeddedListNode {
    #[inline(always)]
    fn default() -> Self {
        Self {
            prev: null_mut(),
            next: null_mut(),
            l: null_mut(),
        }
    }
}

impl fmt::Debug for EmbeddedListNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(")?;

        if !self.prev.is_null() {
            write!(f, "prev: {:p} ", self.prev)?;
        } else {
            write!(f, "prev: none ")?;
        }

        if !self.next.is_null() {
            write!(f, "next: {:p} ", self.next)?;
        } else {
            write!(f, "next: none ")?;
        }
        write!(f, ")")
    }
}

pub struct EmbeddedList {
    length: u64,
    head: *mut EmbeddedListNode,
    tail: *mut EmbeddedListNode,
    node_offset: usize,
}

unsafe impl Sync for EmbeddedList {}
unsafe impl Send for EmbeddedList {}

impl fmt::Debug for EmbeddedList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{ length: {} ", self.length)?;

        if !self.head.is_null() {
            write!(f, "head: {:?} ", self.head)?;
        } else {
            write!(f, "head: none ")?;
        }

        if !self.tail.is_null() {
            write!(f, "tail: {:?} ", self.tail)?;
        } else {
            write!(f, "tail: none ")?;
        }

        write!(f, "}}")
    }
}

impl EmbeddedList {
    #[inline(always)]
    pub fn new(node_offset: usize) -> Self {
        EmbeddedList {
            length: 0,
            head: null_mut(),
            tail: null_mut(),
            node_offset,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.length = 0;
        self.head = null_mut();
        self.tail = null_mut();
    }

    #[inline(always)]
    fn to_item_mut<T>(&self, data: *mut EmbeddedListNode) -> *mut T {
        let off = unsafe { transmute::<*mut EmbeddedListNode, usize>(data) };
        (off - self.node_offset) as *mut T
    }

    #[inline(always)]
    pub fn get_length(&self) -> u64 {
        return self.length;
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        return self.length as usize;
    }

    #[inline(always)]
    pub fn remove(&mut self, del: &mut EmbeddedListNode) {
        if del.l != self {
            return;
        }
        self.remove_node(del);
    }

    #[inline(always)]
    fn remove_node(&mut self, del: &mut EmbeddedListNode) {
        let prev = del.prev;
        let next = del.next;
        if prev.is_null() {
            self.head = next;
        } else {
            unsafe {
                (*prev).next = next;
            }
            del.prev = null_mut();
        }
        if next.is_null() {
            self.tail = prev;
        } else {
            unsafe {
                (*next).prev = prev;
            }
            del.next = null_mut();
        }
        del.l = null_mut();
        debug_assert!(del.next.is_null());
        debug_assert!(del.prev.is_null());
        self.length -= 1;
    }

    #[inline(always)]
    pub fn peak(&mut self, node: &mut EmbeddedListNode) {
        let node_raw = node as *mut EmbeddedListNode;
        let head = self.head;
        if head == node_raw {
            return;
        }
        debug_assert!(!head.is_null());
        // remove node from current position
        let prev = node.prev;
        let next = node.next;
        debug_assert!(!prev.is_null());
        unsafe {
            (*head).prev = node_raw;
            (*prev).next = next;
            if next.is_null() {
                self.tail = prev;
            } else {
                (*next).prev = prev;
            }
        }
        // insert to head
        node.next = head;
        node.prev = null_mut();
        self.head = node_raw;
    }

    #[inline]
    pub fn push_front(&mut self, new_node: &mut EmbeddedListNode) {
        let head = self.head;
        new_node.next = head;
        new_node.l = self as *mut EmbeddedList;
        new_node.prev = null_mut();
        if head.is_null() {
            self.tail = new_node as *mut EmbeddedListNode;
        } else {
            unsafe {
                (*head).prev = new_node as *mut EmbeddedListNode;
            }
        }
        self.head = new_node as *mut EmbeddedListNode;
        self.length += 1;
    }

    #[inline]
    pub fn push_back(&mut self, new_node: &mut EmbeddedListNode) {
        let tail = self.tail;
        // push back node, set node.prev = list.tail and node.next = None
        new_node.prev = tail;
        new_node.l = self as *mut EmbeddedList;
        new_node.next = null_mut();

        if tail.is_null() {
            self.head = new_node as *mut EmbeddedListNode;
        } else {
            unsafe {
                (*tail).next = new_node as *mut EmbeddedListNode;
            }
        }
        self.tail = new_node as *mut EmbeddedListNode;
        self.length += 1;
    }

    #[inline]
    pub fn pop_back<T>(&mut self) -> Option<*mut T> {
        if self.tail.is_null() {
            None
        } else {
            let item = self.to_item_mut(self.tail);
            self.remove_node(unsafe { transmute(self.tail) });
            Some(item)
        }
    }

    #[inline]
    pub fn pop_front<T>(&mut self) -> Option<*mut T> {
        if self.head.is_null() {
            None
        } else {
            let item = self.to_item_mut(self.head);
            self.remove_node(unsafe { transmute(self.head) });
            Some(item)
        }
    }

    #[inline]
    pub fn get_front<T>(&self) -> Option<&mut T> {
        if self.head.is_null() {
            None
        } else {
            Some(unsafe { transmute(self.to_item_mut::<T>(self.head)) })
        }
    }

    #[inline]
    pub fn get_back<T>(&self) -> Option<&mut T> {
        if self.tail.is_null() {
            None
        } else {
            Some(unsafe { transmute(self.to_item_mut::<T>(self.tail)) })
        }
    }

    #[inline(always)]
    pub fn remove_front(&mut self) {
        if !self.head.is_null() {
            self.remove_node(unsafe { transmute(self.head) });
        }
    }

    #[inline(always)]
    pub fn remove_back(&mut self) {
        if !self.tail.is_null() {
            self.remove_node(unsafe { transmute(self.tail) });
        }
    }

    #[inline(always)]
    pub fn is_front(&self, node: &mut EmbeddedListNode) -> bool {
        if self.head.is_null() {
            return false;
        } else {
            return self.head == node as *mut EmbeddedListNode;
        }
    }

    #[inline]
    pub fn has_node(&self, node: &EmbeddedListNode) -> bool {
        node.l as *const EmbeddedList == self as *const EmbeddedList
    }

    pub fn print<T: std::fmt::Debug>(&self) {
        println!("print list begin! length={}", self.length);
        let mut node = self.head;
        while !node.is_null() {
            unsafe {
                let node_item = self.to_item_mut::<T>(node);
                println!("node={:?}", *node_item);
                node = (*node).next;
            }
        }
        println!("print list end:");
    }

    // NOTE: If you plan on turn the raw pointer to owned, use drain instead
    #[inline(always)]
    pub fn iter<'a, T>(&'a self) -> EmbeddedListIterator<'a, T> {
        EmbeddedListIterator {
            list: self,
            cur: null_mut(),
            phan: Default::default(),
        }
    }

    #[inline(always)]
    pub fn drain<'a, T>(&'a mut self) -> EmbeddedListDrainer<'a, T> {
        EmbeddedListDrainer {
            list: self,
            phan: Default::default(),
        }
    }
}

pub struct EmbeddedListIterator<'a, T> {
    list: &'a EmbeddedList,
    cur: *mut EmbeddedListNode,
    phan: std::marker::PhantomData<T>,
}

unsafe impl<'a, T> Sync for EmbeddedListIterator<'a, T> {}
unsafe impl<'a, T> Send for EmbeddedListIterator<'a, T> {}

impl<'a, T> Iterator for EmbeddedListIterator<'a, T> {
    type Item = *mut T;

    fn next(&mut self) -> Option<*mut T> {
        if self.cur == null_mut() {
            if self.list.head == null_mut() {
                return None;
            } else {
                self.cur = self.list.head;
                Some(self.list.to_item_mut::<T>(self.cur))
            }
        } else {
            let next = unsafe { (*self.cur).next };
            if next == null_mut() {
                None
            } else {
                self.cur = next;
                Some(self.list.to_item_mut::<T>(self.cur))
            }
        }
    }
}

pub struct EmbeddedListDrainer<'a, T> {
    list: &'a mut EmbeddedList,
    phan: std::marker::PhantomData<T>,
}

unsafe impl<'a, T> Sync for EmbeddedListDrainer<'a, T> {}
unsafe impl<'a, T> Send for EmbeddedListDrainer<'a, T> {}

impl<'a, T> Iterator for EmbeddedListDrainer<'a, T> {
    type Item = *mut T;

    #[inline]
    fn next(&mut self) -> Option<*mut T> {
        self.list.pop_front::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub struct IntListNode {
        pub value: i64,
        pub node: EmbeddedListNode,
    }

    impl fmt::Debug for IntListNode {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{} {:#?}", self.value, self.node)
        }
    }

    impl fmt::Display for IntListNode {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.value)
        }
    }

    type IntLinkList = EmbeddedList;

    fn new_intnode(i: i64) -> Box<IntListNode> {
        Box::new(IntListNode {
            node: EmbeddedListNode::default(),
            value: i,
        })
    }

    fn new_intlist() -> IntLinkList {
        let off = std::mem::offset_of!(IntListNode, node);
        EmbeddedList::new(off)
    }

    #[test]
    fn list_push_back() {
        println!();
        let mut l = new_intlist();
        println!("empty list:{:?}", l);
        let mut node1 = new_intnode(1);
        l.push_back(&mut node1.node);
        let mut node2 = new_intnode(2);
        l.push_back(&mut node2.node);
        let mut node3 = new_intnode(3);
        l.push_back(&mut node3.node);
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        assert_eq!(3, l.get_length());

        // node1.prev = None and node1.next = node2
        assert!(node1.node.prev.is_null());
        assert_eq!(node1.node.next, &mut node2.node as *mut EmbeddedListNode);

        // node2.prev = node1 and node2.next = node3
        assert_eq!(node2.node.prev, &mut node1.node as *mut EmbeddedListNode);
        assert_eq!(node2.node.next, &mut node3.node as *mut EmbeddedListNode);

        // node3.prev = node2 and node3.next = None
        assert_eq!(node3.node.prev, &mut node2.node as *mut EmbeddedListNode);
        assert!(node3.node.next.is_null());

        assert!(l.is_front(&mut node1.node));

        // peak node2
        l.peak(&mut node2.node);
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        // node2.prev = None and node2.next = node1
        assert!(node2.node.prev.is_null());
        assert_eq!(node2.node.next, &mut node1.node as *mut EmbeddedListNode);

        // node1.prev = node2 and node1.next = node3
        assert_eq!(node1.node.prev, &mut node2.node as *mut EmbeddedListNode);
        assert_eq!(node1.node.next, &mut node3.node as *mut EmbeddedListNode);

        // node3.prev = node1 and node3.next = None
        assert_eq!(node3.node.prev, &mut node1.node as *mut EmbeddedListNode);
        assert!(node3.node.next.is_null());

        assert!(l.is_front(&mut node2.node));

        // peak node3
        l.peak(&mut node3.node);
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        // node3.prev = None and node3.next = node2
        assert!(node3.node.prev.is_null());
        assert_eq!(node3.node.next, &mut node2.node as *mut EmbeddedListNode);

        // node2.prev = node3 and node2.next = node1
        assert_eq!(node2.node.prev, &mut node3.node as *mut EmbeddedListNode);
        assert_eq!(node2.node.next, &mut node1.node as *mut EmbeddedListNode);

        // node1.prev = node2 and node1.next = None
        assert_eq!(node1.node.prev, &mut node2.node as *mut EmbeddedListNode);
        assert!(node1.node.next.is_null());

        assert!(l.is_front(&mut node3.node));

        // peak node3 again
        l.peak(&mut node3.node);
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        // node3.prev = None and node3.next = node2
        assert!(node3.node.prev.is_null());
        assert_eq!(node3.node.next, &mut node2.node as *mut EmbeddedListNode);

        // node2.prev = node3 and node2.next = node1
        assert_eq!(node2.node.prev, &mut node3.node as *mut EmbeddedListNode);
        assert_eq!(node2.node.next, &mut node1.node as *mut EmbeddedListNode);

        // node1.prev = node2 and node1.next = None
        assert_eq!(node1.node.prev, &mut node2.node as *mut EmbeddedListNode);
        assert!(node1.node.next.is_null());

        assert!(l.is_front(&mut node3.node));
    }

    #[test]
    fn list_push_front() {
        println!();
        let mut l = new_intlist();
        println!("empty list:{:?}", l);
        let mut node3 = new_intnode(3);
        l.push_front(&mut node3.node);
        let mut node2 = new_intnode(2);
        l.push_front(&mut node2.node);
        let mut node1 = new_intnode(1);
        l.push_front(&mut node1.node);
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        assert_eq!(3, l.get_length());

        // node1.prev = None and node1.next = node2
        assert!(node1.node.prev.is_null());
        assert_eq!(node1.node.next, &mut node2.node as *mut EmbeddedListNode);

        // node2.prev = node1 and node2.next = node3
        assert_eq!(node2.node.prev, &mut node1.node as *mut EmbeddedListNode);
        assert_eq!(node2.node.next, &mut node3.node as *mut EmbeddedListNode);

        // node3.prev = node2 and node3.next = None
        assert_eq!(node3.node.prev, &mut node2.node as *mut EmbeddedListNode);
        assert!(node3.node.next.is_null());

        assert!(l.is_front(&mut node1.node));
    }

    #[test]
    fn list_remove_node() {
        println!();
        let mut l = new_intlist();
        println!("empty list:{:?}", l);
        let mut node1 = new_intnode(1);
        l.push_back(&mut node1.node);
        let mut node2 = new_intnode(2);
        l.push_back(&mut node2.node);
        let mut node3 = new_intnode(3);
        l.push_back(&mut node3.node);
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        assert_eq!(3, l.get_length());

        // remove node2
        l.remove(&mut node2.node);
        assert_eq!(2, l.get_length());
        println!("after remove node2...");
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        // node1.prev = None and node1.next = node3
        assert!(node1.node.prev.is_null());
        assert_eq!(node1.node.next, &mut node3.node as *mut EmbeddedListNode);

        // node2 = None
        assert!(node2.node.prev.is_null());
        assert!(node2.node.next.is_null());

        // node3.prev = node1 and node3.next = None
        assert_eq!(node3.node.prev, &mut node1.node as *mut EmbeddedListNode);
        assert!(node3.node.next.is_null());

        // list.head = node1 and list.tail = node3
        assert_eq!(l.head, &mut node1.node as *mut EmbeddedListNode);
        assert_eq!(l.tail, &mut node3.node as *mut EmbeddedListNode);

        // remove node1
        l.remove(&mut node1.node);
        assert_eq!(1, l.get_length());
        println!("after remove node1...");
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        // node1 = None
        assert!(node1.node.prev.is_null());
        assert!(node1.node.next.is_null());

        // node3.prev = None and node3.next = None
        assert!(node3.node.prev.is_null());
        assert!(node3.node.next.is_null());

        // list.head = node3 and list.tail = node3
        assert_eq!(l.head, &mut node3.node as *mut EmbeddedListNode);
        assert_eq!(l.tail, &mut node3.node as *mut EmbeddedListNode);

        assert!(node1.node.l.is_null());
        // remove node not in this list
        l.remove(&mut node1.node);
        assert_eq!(1, l.get_length());

        // remove node3
        l.remove(&mut node3.node);
        assert_eq!(0, l.get_length());
        println!("after remove node3...");
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        // node3.prev = None and node3.next = None
        assert!(node3.node.prev.is_null());
        assert!(node3.node.next.is_null());

        // list.head = None and list.tail = None
        assert!(l.head.is_null());
        assert!(l.tail.is_null());
    }

    #[test]
    fn list_pop_front() {
        println!();
        let mut l = new_intlist();
        println!("empty list:{:?}", l);
        let mut node1 = new_intnode(1);
        l.push_back(&mut node1.node);
        let mut node2 = new_intnode(2);
        l.push_back(&mut node2.node);
        let mut node3 = new_intnode(3);
        l.push_back(&mut node3.node);
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        let del_node = l.pop_front::<IntListNode>();
        assert_eq!(2, l.get_length());
        println!("after pop front...");
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        assert!(del_node.is_some());
        unsafe {
            assert_eq!((*del_node.unwrap()).value, 1);
        }

        // list.head = node2 and list.tail = node3
        assert_eq!(l.head, &mut node2.node as *mut EmbeddedListNode);
        assert_eq!(l.tail, &mut node3.node as *mut EmbeddedListNode);
    }

    #[test]
    fn list_pop_back() {
        println!();
        let mut l = new_intlist();
        println!("empty list:{:?}", l);
        let mut node1 = new_intnode(1);
        l.push_back(&mut node1.node);
        let mut node2 = new_intnode(2);
        l.push_back(&mut node2.node);
        let mut node3 = new_intnode(3);
        l.push_back(&mut node3.node);
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        let del_node = l.pop_back::<IntListNode>();
        assert_eq!(2, l.get_length());
        println!("after pop back...");
        println!("list:{:?}", l);
        l.print::<IntListNode>();

        // del node is node3
        assert!(del_node.is_some());
        unsafe {
            assert_eq!((*del_node.unwrap()).value, 3);
        }

        // list.head = node1 and list.tail = node2
        assert_eq!(l.head, &mut node1.node as *mut EmbeddedListNode);
        assert_eq!(l.tail, &mut node2.node as *mut EmbeddedListNode);
    }

    #[test]
    fn list_iter() {
        println!();
        let mut l = new_intlist();
        let mut count = 0;
        for _item in l.iter::<IntListNode>() {
            count += 1;
        }
        assert_eq!(count, 0);
        let mut node1 = new_intnode(1);
        l.push_back(&mut node1.node);
        let mut node2 = new_intnode(2);
        l.push_back(&mut node2.node);
        let mut node3 = new_intnode(3);
        l.push_back(&mut node3.node);
        for _item in l.iter::<IntListNode>() {
            count += 1;
            println!("{}", unsafe { (*_item).value });
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn push_front_pop_front_one() {
        let mut l = new_intlist();
        let mut node1 = new_intnode(1);
        l.push_front(&mut node1.node);
        let del_node = l.pop_front::<IntListNode>();
        unsafe {
            assert_eq!((*del_node.unwrap()).value, 1);
        }
        assert_eq!(0, l.get_length());
        assert!(l.pop_front::<IntListNode>().is_none());
    }

    #[test]
    fn push_front_pop_back_one() {
        let mut l = new_intlist();
        let mut node1 = new_intnode(1);
        l.push_front(&mut node1.node);
        let del_node = l.pop_back::<IntListNode>();
        unsafe {
            assert_eq!((*del_node.unwrap()).value, 1);
        }
        assert_eq!(0, l.get_length());
        assert!(l.pop_front::<IntListNode>().is_none());
    }

    #[test]
    fn push_back_pop_front_one() {
        let mut l = new_intlist();
        let mut node1 = new_intnode(1);
        l.push_back(&mut node1.node);
        let del_node = l.pop_front::<IntListNode>();
        unsafe {
            assert_eq!((*del_node.unwrap()).value, 1);
        }
        assert_eq!(0, l.get_length());
        assert!(l.pop_front::<IntListNode>().is_none());
    }

    #[test]
    fn push_back_pop_back_one() {
        let mut l = new_intlist();
        let mut node1 = new_intnode(1);
        l.push_back(&mut node1.node);
        let del_node = l.pop_back::<IntListNode>();
        unsafe {
            assert_eq!((*del_node.unwrap()).value, 1);
        }
        assert_eq!(0, l.get_length());
        assert!(l.pop_back::<IntListNode>().is_none());
    }
}
