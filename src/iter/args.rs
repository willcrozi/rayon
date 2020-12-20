// Initial attempt with MapArgs as a struct.

#![allow(unreachable_pub)]

use crossbeam_utils::sync::ShardedLock;

use std::{slice, mem, cmp};
use std::iter;
use std::ops::Range;
use std::sync::Arc;
use std::ptr::{drop_in_place, NonNull};
use std::marker::PhantomData;
use std::alloc::{self, Layout};


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


pub struct PartialBuffer<I: Iterator> {
    len: usize,
    inner: Arc<ShardedLock<PartialBufferInner<I>>>,
}

impl<I> PartialBuffer<I>
where
    I: ExactSizeIterator,
{
    fn new(iter: I) -> PartialBuffer<I> {
        let len = iter.len();
        let vec = Vec::<I::Item>::with_capacity(len);
        let buf = vec[..].as_ptr() as *mut I::Item;
        mem::forget(vec);

        let inner = PartialBufferInner { iter, ptr: buf, uninit: 0..len, _marker: PhantomData };

        PartialBuffer { len, inner: Arc::new(ShardedLock::new(inner)) }
    }

    fn get(&self, index: usize) -> &I::Item {
        assert!(index < self.len);

        let inner = self.inner.read().unwrap();
        if !inner.uninit.contains(&index) {
            unsafe { return &*inner.ptr.offset(index as isize) }
        }
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Partial Buffer

// TODO
//  * handle ZSTs
//  * determine if wen should use NonNull
//  * manual allocation/deallocation

struct PartialBufferInner<I: Iterator>
{
    iter: I,
    ptr: *mut I::Item,
    cap: usize,
    uninit: Range<usize>,
    _marker: PhantomData<I::Item>,
}

impl<I> PartialBufferInner<I>
where
    I: ExactSizeIterator + DoubleEndedIterator,
{
    unsafe fn alloc(&mut self) {
        let elem_size = mem::size_of::<I::Item>();
        let
    }

    unsafe fn fill_front(&mut self, mut count: usize)
    {
        let offset = self.uninit.start as isize;
        // TODO debug_asserts here

        let slice = slice::from_raw_parts_mut(self.ptr.offset(offset), count);
        slice.iter_mut()
            .zip(self.iter.by_ref())
            .for_each(|(dest, val)| *dest = val);

        self.uninit.start += count;
    }

    unsafe fn fill_back(&mut self, mut count: usize)
    {
        let offset = (self.uninit.end - count) as isize;
        // TODO debug_asserts here

        let slice = slice::from_raw_parts_mut(self.ptr.offset(offset), count);
        slice.iter_mut().rev()
            .zip(self.iter.by_ref().rev())
            .for_each(|(dest, val)| *dest = val);

        self.uninit.start += count;
    }

    // unsafe fn get(&self, index: usize) -> *const T {
    //     // Check if value at `index` is filled.
    //     if self.uninit.contains(&index) {
    //         // Find which direction would fill soonest.
    //         let fwd = (index - self.uninit.start) + 1;
    //         let back = self.uninit.end - index;
    //
    //         if fwd <= back {
    //             self.
    //         }
    //     }
    //
    //     self.buf.offset(index as isize)
    // }

    // unsafe fn slice
}

impl<I: Iterator> Drop for PartialBufferInner<I> {
    fn drop(&mut self) {
        let elem_size = mem::size_of::<I::Item>();
        if self.cap == 0 || elem_size == 0 {
            // Zero sized, nothing to be done.
            return;
        }

        unsafe {
            // Drop the filled items at the front.
            slice::from_raw_parts_mut(self.ptr, self.uninit.start)
                .iter_mut()
                .for_each(|val| {
                    drop_in_place(val as *mut I::Item)
                });

            // Drop the filled items at the back.
            let count = self.cap - self.uninit.end;
            slice::from_raw_parts_mut(self.ptr.offset(self.uninit.end as isize), count)
                .iter_mut()
                .for_each(|val| {
                    drop_in_place(val as *mut I::Item)
                });

            // Deallocate the buffer memory.
            // let c: NonNull<I::Item> = self.buf.into();
            let layout = Layout::array::<I::Item>(self.cap).unwrap();
            alloc::dealloc(self.ptr as *mut u8, layout);
        }
    }
}

