use crossbeam_utils::sync::{ShardedLock, ShardedLockReadGuard};

use std::{slice, mem, cmp};
use std::fmt::{self, Debug};
use std::ops::Range;
use std::alloc::{self, Layout};
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::mem::MaybeUninit;

/// A lazy thread-safe cache for double-ended exact-sized iterators that allows shared access to
/// sub-slices/iterators of its contents, filling from its source iterator as required.
///
/// Optimised for reads. Currently allocates the its entire storage requirement on the first
/// read request.
#[derive(Clone, Debug)]
pub struct IterCache<I: Iterator> {
    len: usize,
    inner: Arc<ShardedLock<IterCacheInner<I>>>,
}

impl<I> IterCache<I>
    where
        I: ExactSizeIterator + DoubleEndedIterator,
{
    fn new(iter: I) -> IterCache<I> {
        let len = iter.len();
        let inner = IterCacheInner::new(iter);

        IterCache { len, inner: Arc::new(ShardedLock::new(inner)) }
    }

    /// Fill's enough items from the source iterator in order to provide read access to the items
    /// at indexes within `range`. Returns a the read-lock on `inner` to allow callers immediate
    /// read access.
    ///
    /// **Safety:** `range` must not exceed the capacity of this cache (which is the original
    /// length of the source iterator when this cache was constructed.
    unsafe fn fill(&self, range: &Range<usize>)
                   -> ShardedLockReadGuard<'_, IterCacheInner<I>>
    {
        // TODO move some of this into inner `fill` method?

        { // Read lock.
            let inner = self.inner.read().unwrap();

            // The requested range's end must be less than our capacity, or less than the untouched
            // iterator's length (if we have not allocated yet).
            debug_assert!(
                range.end <= inner.cap
                || (inner.cap == 0 && range.end <= inner.iter.len())
            );

            // Fast path for read-only cases (i.e. buffer is filled for all indexes in `range`).
            if range.is_empty() || inner.is_filled(range) { return inner; }

        } // Read unlock.

        // We need more elements from source iterator.
        { // Write lock.
            let mut inner = self.inner.write().unwrap();

            // Second check in case our requested range was filled in between dropping read lock
            // and acquiring the write lock.
            if inner.is_filled(range) {
                // This time we need to drop the write lock and reacquire the read lock.
                drop(inner);
                return self.inner.read().unwrap();
            }

            // Allocate memory if needed.
            let zero_sized = mem::size_of::<I::Item>() == 0;
            if !zero_sized && inner.cap == 0 {
                let cap = inner.iter.len();
                inner.alloc(cap);
            }

            debug_assert!(range.end <= inner.cap);

            // Find the first and last positions within the requested range that are not yet filled.
            let range_fill_start = cmp::max(range.start, inner.empty.start);
            let range_fill_end = cmp::min(range.end, inner.empty.end);

            if range_fill_start > range_fill_end {
                println!("range: {:?}", &range);
                println!("inner.empty: {:?}", &inner.empty);
                panic!();
            }

            // Count of items within requested range that need filling.
            debug_assert!(range_fill_start <= range_fill_end);
            let range_fill_count = range_fill_end - range_fill_start;

            // Now we can safely (without risk of underflow) find the gaps - items not in the range
            // that would need filling, for both fill directions.
            let f_gap = range_fill_start - inner.empty.start;
            let b_gap = inner.empty.end - range_fill_end;

            // If there is no front-gap (requested range is overlapping/contiguous with filled area
            // at front) or if the front gap is smaller than the back gap after adjusting for a
            // a preference to front-fill, then we will from the front.
            const F_WEIGHT: usize = 0;
            if f_gap == 0 || f_gap < (b_gap + F_WEIGHT) {
                inner.fill_front(range_fill_count + f_gap);
            } else {
                let first_empty = cmp::max(range.start, inner.empty.start);
                inner.fill_back(range_fill_count + b_gap);
            }
        } // Write unlock.

        self.inner.read().unwrap()
    }

    /// TODO doc
    pub fn get(&self, index: usize) -> &I::Item {
        unsafe {
            let inner = self.fill(&(index..index + 1));
            let ptr = inner.ptr.as_ptr().offset(index as isize);
            &*ptr
        }
    }

    /// TODO doc
    pub fn slice(&self, range: Range<usize>) -> &[I::Item] {
        unsafe {
            let inner = self.fill(&range);
            let raw_ptr = inner.ptr.as_ptr().offset(range.start as isize);
            let ptr = NonNull::new_unchecked(raw_ptr);

            slice::from_raw_parts(ptr.as_ptr(), range.len())
        }
    }

    /// TODO doc
    #[inline]
    pub fn iter_range(&self, range: Range<usize>) -> slice::Iter<'_, I::Item> {
        self.slice(range).iter()
    }

    /// TODO doc
    #[inline]
    pub fn iter(&self) -> slice::Iter<'_, I::Item> {
        self.slice(0..self.len).iter()
    }
}


// NOTE: we can't use reallocation since we share slices before we are filling the buffer.
struct IterCacheInner<I: Iterator>
{
    iter: I,
    ptr: NonNull<I::Item>,
    cap: usize,
    empty: Range<usize>,
}

impl<I: Iterator + Debug> Debug for IterCacheInner<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IterCacheInner")
            .field("iter", &self.iter)
            .field("ptr", &self.ptr)
            .field("cap", &self.cap)
            .field("empty", &self.empty)
            .finish()
    }
}

unsafe impl<I: Iterator> Sync for IterCacheInner<I> {}
unsafe impl<I: Iterator> Send for IterCacheInner<I> {}

impl<I> IterCacheInner<I>
    where
        I: ExactSizeIterator + DoubleEndedIterator,
{
    fn new(iter: I) -> Self {
        let ptr = NonNull::dangling();
        // TODO handle rare case where iterator length is usize::MAX (Range will not handle this),
        //      switch to RangeInclusive?
        let (cap, empty) = match mem::size_of::<I::Item>() {
            // Zero-sized type. No allocation needed.
            0 => (iter.len(), 0..iter.len()),
            // Max-out empty range until allocation (saves check for zero capacity).
            _ => (0, 0..usize::MAX),
        };

        IterCacheInner { iter, ptr, cap, empty }
    }

    unsafe fn alloc(&mut self, cap: usize) {
        // ZSTs: If I::Item is zero-sized then we will never reach here.
        debug_assert!(mem::size_of::<I::Item>() != 0);

        // Allocation must only happen once.
        debug_assert!(self.cap == 0);

        let layout = Layout::array::<I::Item>(cap).unwrap();
        let ptr = alloc::alloc(layout) as *mut I::Item;

        self.ptr = NonNull::new(ptr).expect("memory allocation failure");
        self.cap = cap;
        self.empty = 0..cap;
    }

    /// Check the indexes in `range` returning `true` if they are filled, `false` otherwise.
    /// Empty `range` and zero-capacity checks must be performed separately if needed.
    #[inline]
    fn is_filled(&self, range: &Range<usize>) -> bool {
            self.empty.is_empty()
            || range.end <= self.empty.start
            || range.start >= self.empty.end
    }

    unsafe fn fill_front(&mut self, count: usize) {
        debug_assert!(count <= self.empty.len());

        let offset = self.empty.start as isize;
        let ptr = self.ptr.as_ptr().offset(offset);

        for (i, val) in (0..count).zip(self.iter.by_ref()) {
            ptr.offset(i as isize).write(val);
        }

        self.empty.start += count;
    }

    unsafe fn fill_back(&mut self, count: usize) {
        debug_assert!(count <= self.empty.len());

        let offset = (self.empty.end - count) as isize;
        let ptr = self.ptr.as_ptr().offset(offset);

        for (i, val) in (0..count).zip(self.iter.by_ref().rev()) {
            ptr.offset(-(i as isize)).write(val);
        }

        self.empty.end -= count;
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
            let count = self.empty.start;
            let front = ptr::slice_from_raw_parts_mut(ptr, count);
            ptr::drop_in_place(front);

            // Drop the filled items at the back.
            let ptr = self.ptr.as_ptr().offset(self.empty.end as isize);
            let count = self.cap - self.empty.end;
            let back = ptr::slice_from_raw_parts_mut(ptr, count);
            ptr::drop_in_place(back);

            // Deallocate the buffer memory.
            let layout = Layout::array::<I::Item>(self.cap).unwrap();
            let ptr = self.ptr.as_ptr() as *mut u8;
            alloc::dealloc(ptr, layout);
        }
    }
}

#[cfg(test)]
mod test {
    use std::{thread, mem};
    use std::time::Duration;
    use super::IterCache;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // TODO drop test

    #[test]
    fn iter_cache() {
        const ITER_LEN: usize = 1000;
        const CHUNK_SIZE: usize = 100;

        let iter = (0..ITER_LEN).map(|n| {
            // println!(".map threadid: {:?}", thread::current().id());
            thread::sleep(Duration::from_micros(200));
            n
        });

        let cache = Box::new(IterCache::new(iter));
        let mut handles = vec![];

        for _ in 0..=16 {
            let cache = cache.clone();

            let handle = thread::spawn(move || {
                for i in 0..(ITER_LEN  - CHUNK_SIZE) {
                    // Front
                    let f_range = i..(i + CHUNK_SIZE);
                    let f_iter = cache.iter_range(f_range.clone());

                    let result = cache.slice(f_range.clone());
                    let mut expected = f_range.collect::<Vec<_>>();

                    // println!("{:?}", &result);
                    // assert!(iter_eq(f_iter, expected.iter()));
                    assert_eq!(&result, &expected);

                    // Back
                    let base = (ITER_LEN - CHUNK_SIZE) - i;
                    let b_range = base..(base + CHUNK_SIZE);
                    let b_iter = cache.iter_range(b_range.clone());

                    let result = cache.slice(b_range.clone());
                    let expected = b_range.collect::<Vec<_>>();

                    // println!("{:?}", &result);
                    // assert!(iter_eq(b_iter, expected.iter()));
                    assert_eq!(&result, &expected);

                    // thread::sleep(Duration::from_micros(10000));
                }
            });

            handles.push(handle);
        }

        for t in handles {
            t.join().unwrap();
        }
    }

    #[test]
    fn iter_cache_drop() {
        struct DropMe<'a> { counter: &'a AtomicUsize }
        struct DropMeZst { }

        let drop_count = AtomicUsize::new(0);

        impl<'a> Drop for DropMe<'a> {
            fn drop(&mut self) {
                let prev = self.counter.fetch_add(1, Ordering::SeqCst);
                eprintln!("Dropping, count was: {}, now: {}", prev, prev + 1);
            }
        }

        const LEN: usize = 32;

        let mut v = vec![];
        for i in 0..LEN {
            v.push(DropMe { counter: &drop_count });
        }

        let iter = IterCache::new(v.into_iter());
        let v2 = iter.iter().collect::<Vec<_>>();
        mem::drop(iter);

        assert_eq!(drop_count.load(Ordering::SeqCst), LEN);

    }

    ////////////////////////////////////////////////////////////////////////////

    // TODO fix this (look at itertools test macros?
    fn iter_eq<I, J>(a: I, mut b: J) -> bool
        where
            I: Iterator,
            I::Item: PartialEq,
            J: Iterator<Item=I::Item>,
    {
        let mut pos = 0;
        for a_item in a {
            match b.next() {
                Some(b_item) => if a_item != b_item {
                    eprintln!("Items at position {} are not equal.", &pos);
                    return false;
                },
                None => return false,
            }
            pos += 1;
        }

        true
    }
}