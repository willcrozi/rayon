// Initial attempt with MapArgs as a struct.

#![allow(unreachable_pub)]

use crossbeam_utils::sync::ShardedLock;

use std::slice;
use std::iter;
use std::ops::Range;
use std::sync::Arc;



// We need three distinct objects:
//
//  * An initial 'source' of the arguments.
//    Held by: Producer (alongside base iterator and owned F)
//
//  * A splittable version

pub trait Args<'a>: Sync + Sized
{
    type Item;
    type Iter: Iterator<Item=Self::Item> + 'a;

    fn len(&self) -> usize;

    // to support PartialArgs::pop do we implement Index???
    // or fn get(&self, index: usize)

    // Args must be Sync (what about Send?), the impl for I: Iterator will need to use Arc<RwLock...>
    // or similar...

    // Do we offer iterator provider(s) here, if so do we have two associated types for full and partial???
    // Or do we just stick with implementing Index then slice and let PartialEq iterate that and
    // clone Ts

    // fn range_iter<I: SliceIndex<Self>>(&'t self, range: Range<I>) -> Cloned<Iter<'t, Self::Item>>;
    // fn map_iter<'f, F, T>(self, map_op: F) -> MapArgsIter<'f, F, T, Self>;

    fn get(&self, index: usize) -> &Self::Item;
    fn iter_range(&'a self, range: Range<usize>) -> Self::Iter;
}

pub struct PartialArgs<'a, A: Args<'a>, > {
    args: &'a A,
    range: Range<usize>,
}

impl<'a, A> PartialArgs<'a, A>
where
    A: Args<'a>,
    A::Item: Clone,
{
    #[inline]
    pub fn len(&self) -> usize { self.range.len() }

    // TODO maybe implement try_pop...
    pub fn pop(&mut self) -> A::Item {
        debug_assert!(self.len() > 1);

        let index = self.range.start;
        self.range.start += 1;

        self.args.get(index).clone()
    }

    pub fn split_at(&mut self, index: usize) -> (Self, Self) {
        debug_assert!(self.len() >= index);
        let split = self.range.start + index;

        (
            PartialArgs { args: self.args, range: self.range.start..split },
            PartialArgs { args: self.args, range: split..self.range.end },
        )
    }
}

impl<'a, A: Args<'a>> IntoIterator for PartialArgs<'a, A>
where
    A::Item: Clone,
{
    type Item = A::Item;
    type IntoIter = A::Iter;

    fn into_iter(self) -> Self::IntoIter {
        self.args.iter_range(self.range)
    }
}

////////////////////////////////////////////////////////////////////////////////
// SliceArgs

pub struct SliceArgs<'a, T>(&'a [T]);

impl<'a, T: Sync + Clone> Args<'a> for SliceArgs<'a, T> {
    type Item = T;
    type Iter = iter::Cloned<slice::Iter<'a, T>>;

    #[inline]
    fn len(&self) -> usize { self.0.len() }

    #[inline]
    fn get(&self, index: usize) -> &Self::Item { &self.0[index] }

    #[inline]
    fn iter_range(&'a self, range: Range<usize>) -> Self::Iter { self.0[range].iter().cloned() }
}

// impl<'a, T: Sync + Clone + 'a> Args<'a> for Vec<T>
// {
//     type Item = T;
//     type Iter = iter::Cloned<slice::Iter<'a, T>>;
//
//     #[inline]
//     fn len(&self) -> usize { Vec::len(self) }
//
//     #[inline]
//     fn get(&self, index: usize) -> &Self::Item { &self[index] }
//
//     #[inline]
//     fn iter_range(&'a self, range: Range<usize>) -> Self::Iter { self[range].iter().cloned() }
// }

////////////////////////////////
// Iterator source adaptor

pub struct IterArgs<I: Iterator> {
    inner: Arc<ShardedLock<IterArgsInner<I>>>
}

struct IterArgsInner<I: Iterator> {
    src: I,
    buf: Vec<I::Item>,
}

impl<'a, I> Args<'a> for IterArgs<I>
where
    I: ExactSizeIterator + DoubleEndedIterator + Send + Sync,
    I::Item: Clone + Send + Sync + 'a,
{
    type Item = I::Item;
    type Iter = iter::Cloned<slice::Iter<'a, I::Item>>;

    fn len(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.src.len() + inner.buf.len()
    }

    fn get(&self, index: usize) -> &Self::Item {
        let inner = self.inner.read().unwrap();

        // Sanity check on index.
        debug_assert!(index < (inner.src.len() + inner.buf.len()));

        // Return the ref to the element if we have it buffered.
        if index < inner.buf.len() {
            return &inner.buf[index];
        }

        // Swap the read lock for a write lock.
        drop(inner);
        let mut inner = self.inner.write().unwrap();

        // Lazy buffer fill if required.
        if index >= inner.buf.len() {
            let delta = (index - inner.buf.len()) + 1;
            // let extra = inner.src.by_ref().take(delta);
            inner.buf.extend(inner.src.by_ref().take(delta));
            debug_assert!(inner.buf.len() > index);
        }

        // NOTE:It seems theres no way to do this without a type with an unsafe implementation
        //      allowing slices to be created from an area of memory still being written to. By
        //      carefully tracking the uninitialised area(s) this could be done.
        //      * Allocate the capacity needed to hold all items from the iterator and place behind
        //        a raw pointer.
        //      * Populate from a double-ended, exact-sized iterator, keeping track of uninitialised
        //        middle range.
        //      * Protect state (empty range) using a read-write lock.
        //

        &inner.buf[index]
    }

    fn iter_range(&'a self, range: Range<usize>) -> Self::Iter {
        let inner = self.inner.read().unwrap();
        debug_assert!(range.start <= inner.buf.len() && range.end <= inner.buf.len());
        inner.buf[range].iter().cloned()
    }
}

// impl<'a, I> Args<'a> for I
// where
//     I: ExactSizeIterator + DoubleEndedIterator,
// {
//     type Item = I::Item;
//     type Iter = iter::Cloned<slice::Iter<'a, I::Item>>;
//
//     fn len(&self) -> usize { <Self as ExactSizeIterator>::len() }
//
//     fn get(&self, index: usize) -> &Self::Item {
//         unimplemented!()
//     }
//
//     fn iter_range(&'a self, range: Range<usize>) -> Self::Iter {
//         unimplemented!()
//     }
// }










////////////////////////////////////////////////////////////////////////////////
// Junked:

// #[derive(Clone)]
// enum PartialArgs<'a, A: Args> {
//     Ref(&'a A, Range<usize>),
//     Owned(A),
// }

// impl<'a, A> PartialArgs<'a, A>
//     where
//         A: Clone,
//         A: Args + 'a,
// {
//     type Item = A::Item;
//     type SubRange = A::SubRange;
//
//     fn len(&self) -> usize {
//         match self {
//             PartialArgs::Ref(_, range) => { range.len() }
//             PartialArgs::Owned(args) => { args.len() }
//         }
//     }
//
//     fn split_at(self, index: usize) -> (Self, Self) {
//         match self {
//             PartialArgs::Ref(args, range) => {
//                 debug_assert!(index <= range.len());
//                 let mid = range.start + index;
//                 (
//                     PartialArgs::Ref(args, range.start..mid),
//                     PartialArgs::Ref(args, mid..range.end)
//                 )
//             }
//             PartialArgs::Owned(args) => {
//                 let (left, right) = args.split_at(index);
//                 (PartialArgs::Owned(left), PartialArgs::Owned(right))
//             }
//         }
//     }
//
//     fn pop(&mut self) -> Self::Item {
//         match self {
//             PartialArgs::Ref(args, range) => {
//                 let mut args = args.sub_range(range.clone());
//                 let result = args.pop();
//                 *self = PartialArgs::Owned(args);
//                 result
//             }
//             PartialArgs::Owned(args) => { args.pop() }
//         }
//     }
//
//     fn sub_range(&self, sub_range: Range<usize>) -> Self::SubRange {
//         match self {
//             PartialArgs::Ref(args, range) => {
//                 let start = range.start + sub_range.start;
//                 let end = range.start + sub_range.end;
//
//                 PartialArgs::Ref(args, start..end)
//             }
//             PartialArgs::Owned(args) => {
//                 PartialArgs::Owned(args.sub_range(sub_range))
//             }
//         }
//     }
// }

// impl<'a, A: Args> IntoIterator for PartialArgs<'a, A> {
//     type Item = A::Item;
//     type IntoIter = A::IntoIter;
//
//     fn into_iter(self) -> Self::IntoIter {
//         match self {
//             PartialArgs::Ref(args, range) => { args.sub_range(range).into_iter() }
//             PartialArgs::Owned(args) => { args.into_iter() }
//         }
//     }
// }

////////////////////////////////////////////////////////////////////////////////

// impl<'t, T> Args for &'t[T] {
//     type Item = &'t T;
//     type SubRange = slice::Iter<'t, T>;
//
//     fn len(&self) -> usize { <[T]>::len(&self) }
//
//     fn split_at(self, index: usize) -> (Self, Self) {
//         (&self[0..index], &self[index..0])
//     }
//
//     fn pop(&mut self) -> Self::Item {
//         let result = &self[0];
//         *self = &self[1..];
//         result
//     }
//
//     fn sub_range(&self, range: Range<usize>) -> Self::SubRange {
//         self[&range].iter()
//     }
// }
//
// impl<'t, T> Args for Vec<T>
// where
//     T: Clone,
// {
//     type Item = T;
//     type SubRange = vec::IntoIter<T>;
//
//     fn len(&self) -> usize { <[T]>::len(&self) }
//
//     fn split_at(mut self, index: usize) -> (Self, Self) {
//         let right = self.split_off(index);
//         (self, right)
//     }
//
//     fn pop(&mut self) -> Self::Item {
//         self.pop().unwrap()
//     }
//
//     fn sub_range(&self, range: Range<usize>) -> Self::SubRange {
//         self[&range]
//     }
// }

// impl<I> Args for I
// where
//     I: ExactSizeIterator,
// {
//     fn len(&self) -> usize {
//         self.len()
//     }
//
//     fn split_at(self, index: usize) -> (Self, Self) {
//         unimplemented!()
//     }
//
//     fn pop(&mut self) -> Self::Item {
//         self.next().unwrap()
//     }
//
//     fn sub_range(&self, range: Range<usize>) -> Self {
//         unimplemented!()
//     }
// }



















////////////////////////////////////////////////////////////////////////////////

// struct MapArgsIter<'f, F, T, A: Args> {
//     map_op: &'f F,
//     item: T,
//     args: A::IntoIter,
// }
//
// impl<'f, F, T, A, R> Iterator for MapArgsIter<'f, F, T, A>
// where
//     F: Fn(T, A::Item) -> R,
//     A: Args,
//     T: Clone,
// {
//     type Item = R;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         unimplemented!()
//     }
// }


// TODO Using a struct here might be more restrictive than using a trait. For example it might be
//      possible to perform splits in a more efficient way when implementing a trait for some
//      provded type here
// pub(crate) struct MapArgs<'f, F, T, A> {
//     item: T,
//     map_op: &'f F,
//     args: A,
// }
//
// impl<'f, F, T, A, R> MapArgs<'f, F, T, A>
//     where
//         F: Fn(T, A::Item) -> R,
//         T: Clone,
//         A: Args,
//         A::IntoIter: ExactSizeIterator,
// {
//     pub fn split_at(self, index: usize) -> (Self, Self) {
//         let (l_args, r_args) = self.args.split_at(index);
//         (
//             MapArgs { item: self.item.clone(), map_op: self.map_op, args: l_args },
//             MapArgs { item: self.item, map_op: self.map_op, args: r_args },
//         )
//     }
//
//     pub fn pop(&mut self) -> R {
//         (self.map_op)(self.item.clone(), self.args.pop())
//     }
// }
//
// impl<'f, F, T, A, R> IntoIterator for MapArgs<'f, F, T, A>
//     where
//         F: Fn(T, A::Item) -> R,
//         T: Clone,
//         A: Args,
//         A::IntoIter: ExactSizeIterator,
// {
//     type Item = R;
//     type IntoIter = MapArgsIter;
//
//     fn into_iter(self) -> Self::IntoIter {
//
//     }
// }
//

