// Initial attempt with MapArgs as a struct.

#![allow(unreachable_pub)]

use std::slice;
use std::ops::Range;
use std::fmt::{self, Debug, Formatter};


// We need three distinct objects:
//
//  * An initial 'source' of the arguments.
//    Held by: Producer (alongside base iterator and owned F)
//
//  * A splittable version
/// A trait representing a list of arguments
pub trait Args<'a>: Sync + Sized
{
    /// The type of the arguments provided by this `Args`.
    type Item: Clone;

    /// The type of the iterator provided by this `Args`.
    type Iter: Iterator<Item=Self::Item> + 'a;

    /// The number of arguments provided by this `Args`.
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

    /// Returns the argument at position `index`.
    fn get(&'a self, index: usize) -> Self::Item;

    /// Returns an iterator over the arguments at the positions within `range`.
    fn iter_range(&'a self, range: Range<usize>) -> Self::Iter;
}


/// Represents a partial set of arguments that can be split and 'popped' (in a stack like fashion).
pub struct PartialArgs<'a, A: Args<'a>, > {
    args: &'a A,
    range: Range<usize>,
}

impl<'a, A: Args<'a> + Debug> Debug for PartialArgs<'a, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialArgs")
            .field("args", &self.args)
            .field("range", &self.range)
            .finish()
    }
}

impl<'a, A> PartialArgs<'a, A>
where
    A: Args<'a>,
    A::Item: Clone,
{
    /// Returns the number of arguments contained.
    #[inline]
    pub fn len(&self) -> usize { self.range.len() }

    // TODO maybe implement try_pop...
    /// Removes the first argument and returns it.
    pub fn pop(&mut self) -> A::Item {
        debug_assert!(self.len() > 1);

        let index = self.range.start;
        self.range.start += 1;

        self.args.get(index).clone()
    }

    /// Splits this set of partial arguments into left and right halves at the position `index`.
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

/// A slice of arguments.
pub struct SliceArgs<'a, T>(&'a [T]);

impl<'a, A: Args<'a> + Debug> Debug for SliceArgs<'a, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SliceArgs").finish()
    }
}

impl<'a, T: Sync + Clone> Args<'a> for SliceArgs<'a, T> {
    type Item = &'a T;
    type Iter = slice::Iter<'a, T>;

    #[inline]
    fn len(&self) -> usize { self.0.len() }

    #[inline]
    fn get(&'a self, index: usize) -> Self::Item { &self.0[index] }

    #[inline]
    fn iter_range(&'a self, range: Range<usize>) -> Self::Iter { self.0[range].iter() }
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

// pub struct IterArgs<I: Iterator> {
//     inner: Arc<ShardedLock<IterArgsInner<I>>>
// }
//
// struct IterArgsInner<I: Iterator> {
//     src: I,
//     buf: Vec<I::Item>,
// }
//
// impl<'a, I> Args<'a> for IterArgs<I>
// where
//     I: ExactSizeIterator + DoubleEndedIterator + Send + Sync,
//     I::Item: Clone + Send + Sync + 'a,
// {
//     type Item = I::Item;
//     type Iter = iter::Cloned<slice::Iter<'a, I::Item>>;
//
//     fn len(&self) -> usize {
//         let inner = self.inner.read().unwrap();
//         inner.src.len() + inner.buf.len()
//     }
//
//     fn get(&self, index: usize) -> &Self::Item {
//         let inner = self.inner.read().unwrap();
//
//         // Sanity check on index.
//         debug_assert!(index < (inner.src.len() + inner.buf.len()));
//
//         // Return the ref to the element if we have it buffered.
//         if index < inner.buf.len() {
//             return &inner.buf[index];
//         }
//
//         // Swap the read lock for a write lock.
//         drop(inner);
//         let mut inner = self.inner.write().unwrap();
//
//         // Lazy buffer fill if required.
//         if index >= inner.buf.len() {
//             let delta = (index - inner.buf.len()) + 1;
//             // let extra = inner.src.by_ref().take(delta);
//             inner.buf.extend(inner.src.by_ref().take(delta));
//             debug_assert!(inner.buf.len() > index);
//         }
//
//         // NOTE:It seems theres no way to do this without a type with an unsafe implementation
//         //      allowing slices to be created from an area of memory still being written to. By
//         //      carefully tracking the uninitialised area(s) this could be done.
//         //      * Allocate the capacity needed to hold all items from the iterator and place behind
//         //        a raw pointer.
//         //      * Populate from a double-ended, exact-sized iterator, keeping track of uninitialised
//         //        middle range.
//         //      * Protect state (empty range) using a read-write lock.
//         //
//
//         &inner.buf[index]
//     }
//
//     fn iter_range(&'a self, range: Range<usize>) -> Self::Iter {
//         let inner = self.inner.read().unwrap();
//         debug_assert!(range.start <= inner.buf.len() && range.end <= inner.buf.len());
//         inner.buf[range].iter().cloned()
//     }
// }




