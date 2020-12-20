use crossbeam_utils::sync::{ShardedLock, ShardedLockReadGuard};

use std::{slice, mem};
use std::fmt::{self, Debug};
use std::ops::Range;
use std::alloc::{self, Layout};
use std::ptr::{NonNull, drop_in_place};
use std::sync::Arc;

/// A lazy thread-safe cache for double-ended exact-sized iterators that allows shared access to
/// sub-slices/iterators of its contents, filling from its source iterator as required.
///
/// Optimised for reads. Currently allocates the its entire storage requirement on the first
/// read request.
#[derive(Debug)]
pub struct IterCache<I: Iterator> {
    inner: Arc<ShardedLock<IterCacheInner<I>>>,
}

impl<I> IterCache<I>
    where
        I: ExactSizeIterator + DoubleEndedIterator,
{
    fn new(iter: I) -> IterCache<I> {
        let inner = IterCacheInner::new(iter);

        IterCache { inner: Arc::new(ShardedLock::new(inner)) }
    }

    // Fill's enough items from the source iterator in order to provide read access to the items
    // at indexes within `range`. Returns a the read-lock on `inner` to allow callers immediate
    // read access.
    unsafe fn fill_required(&self, range: &Range<usize>)
                            -> ShardedLockReadGuard<'_, IterCacheInner<I>>
    {
        let (uninit_start, uninit_end);

        { // Read locked.
            let inner = self.inner.read().unwrap();

            uninit_start = inner.uninit.start;
            uninit_end = inner.uninit.end;

            // Optimise for read-only cases (buffer is filled for all indexes in `range`).
            if uninit_start > range.end || uninit_end <= range.start {
                return inner;
            }
        }

        // We need more elements from source iterator. Calculate the counts for items required at
        // front and back.
        let f_req = range.start.saturating_sub(uninit_start);
        let b_req = uninit_end.saturating_sub(range.end);
        let count = f_req + b_req;

        { // Write locked.
            let mut inner = self.inner.write().unwrap();

            // Prefer filling from front.
            if f_req > 0 {
                inner.fill_front(count);
            } else {
                inner.fill_back(count);
            }
        }

        self.inner.read().unwrap()
    }

    /// TODO doc
    pub fn get(&self, index: usize) -> &I::Item {
        unsafe {
            let inner = self.fill_required(&(index..index + 1));
            let ptr = inner.ptr.as_ptr().offset(index as isize);
            &*ptr
        }
    }

    /// TODO doc
    pub fn slice(&self, range: Range<usize>) -> &[I::Item] {
        unsafe {
            let inner = self.fill_required(&range);
            let ptr = NonNull::new_unchecked(inner.ptr.as_ptr().offset(range.start as isize));

            slice::from_raw_parts(ptr.as_ptr(), range.len())
        }
    }

    /// TODO doc
    #[inline]
    pub fn iter_range(&self, range: Range<usize>) -> slice::Iter<'_, I::Item> {
        self.slice(range).iter()
    }
}


// NOTE: we can't use reallocation since we share slices before we are filling the buffer.
struct IterCacheInner<I: Iterator>
{
    iter: I,
    ptr: NonNull<I::Item>,
    cap: usize,
    uninit: Range<usize>,
}

impl<I: Iterator + Debug> Debug for IterCacheInner<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IterCacheInner")
            .field("iter", &self.iter)
            .field("ptr", &self.ptr)
            .field("cap", &self.cap)
            .field("uninit", &self.uninit)
            .finish()
    }
}

impl<I> IterCacheInner<I>
    where
        I: ExactSizeIterator + DoubleEndedIterator,
{
    fn new(iter: I) -> Self {
        let ptr = NonNull::dangling();
        // TODO handle rare case where iterator length is usize::MAX (Range will not handle this)
        let (cap, uninit) = match mem::size_of::<I::Item>() {
            0 => (usize::MAX, 0..usize::MAX),
            _ => (0, 0..0),
        };

        IterCacheInner { iter, ptr, cap, uninit }
    }

    unsafe fn alloc(&mut self, cap: usize) {
        // ZSTs: If I::Item is zero-sized then we will never reach here, since self.cap will
        // be set to usize::MAX at construction.

        // Allocation must only happen once.
        debug_assert!(self.cap == 0);

        let layout = Layout::array::<I::Item>(cap).unwrap();
        let ptr = alloc::alloc(layout) as *mut I::Item;

        self.ptr = NonNull::new_unchecked(ptr);
        self.uninit = 0..cap;
    }

    unsafe fn fill_front(&mut self, count: usize) {
        if self.cap == 0 {
            self.alloc(self.iter.len())
        }

        debug_assert!(count <= self.uninit.len());

        let offset = self.uninit.start as isize;
        let ptr = self.ptr.as_ptr().offset(offset);
        let slice = slice::from_raw_parts_mut(ptr, count);

        slice.iter_mut()
            .zip(self.iter.by_ref())
            .for_each(|(dest, val)| *dest = val);

        self.uninit.start += count;
    }

    unsafe fn fill_back(&mut self, count: usize) {
        if self.cap == 0 {
            self.alloc(self.iter.len())
        }

        debug_assert!(count <= self.uninit.len());

        let offset = (self.uninit.end - count) as isize;
        let ptr = self.ptr.as_ptr().offset(offset);
        let slice = slice::from_raw_parts_mut(ptr, count);

        slice.iter_mut().rev()
            .zip(self.iter.by_ref().rev())
            .for_each(|(dest, val)| *dest = val);

        self.uninit.end -= count;
    }
}

impl<I: Iterator> Drop for IterCacheInner<I> {
    fn drop(&mut self) {
        let elem_size = mem::size_of::<I::Item>();
        if self.cap == 0 || elem_size == 0 {
            // No allocation, nothing to be done
            return;
        }

        unsafe {
            // Drop the filled items at the front.
            let ptr = self.ptr.as_ptr();
            slice::from_raw_parts_mut(ptr, self.uninit.start)
                .iter_mut()
                .for_each(|val| {
                    drop_in_place(val as *mut I::Item)
                });

            // Drop the filled items at the back.
            let count = self.cap - self.uninit.end;
            let ptr = self.ptr.as_ptr().offset(self.uninit.end as isize);
            slice::from_raw_parts_mut(ptr, count)
                .iter_mut()
                .for_each(|val| {
                    drop_in_place(val as *mut I::Item)
                });

            // Deallocate the buffer memory.
            // let c: NonNull<I::Item> = self.buf.into();
            let layout = Layout::array::<I::Item>(self.cap).unwrap();
            let ptr = self.ptr.as_ptr() as *mut u8;
            alloc::dealloc(ptr, layout);
        }
    }
}