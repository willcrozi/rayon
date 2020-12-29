use crossbeam_utils::sync::{ShardedLock, ShardedLockReadGuard};

use std::{slice, mem, cmp};
use std::fmt::{self, Debug};
use std::ops::Range;
use std::alloc::{self, Layout};
use std::ptr::{self, NonNull};
use std::sync::Arc;

// TODO rename Args: it's not specific to arguments!
/// A trait representing a list of arguments.
///
/// `Args` is intended to be shared (across threads) in order to allow parallel iterator producers
/// to dynamically split and adjust their contents... TODO
pub trait Args<'a>: Sync + Send {
    /// The type of the arguments provided by this `Args`.
    type Item: Clone + 'a;

    /// The type of the iterator provided by this `Args`.
    type Iter: Iterator<Item=&'a Self::Item>;

    /// The number of arguments provided by this `Args`.
    fn len(&self) -> usize;

    /// Returns the argument at position `index`.
    fn get(&'a self, index: usize) -> &'a Self::Item;

    /// Returns an iterator over the arguments at the positions within `range`.
    fn iter_range(&'a self, range: Range<usize>) -> Self::Iter;
}

// TODO maybe default iter type that is used by a default impl. of iter_range (and full iter method?)

// Let's get this straight:
// Args _owns_ the items, whether they be moved in structs or moved-in references

// Approach (1)
//      trait Args {
//          type Item,
//          type Iter: Iterator<Item=Self::Item>
//          fn get<'a>(&'a self, ...) -> &Self::Item;
//          fn iter<'a>(&'a self, ...) -> ?????
// NOTE: problem is that to have Iter yield references we need a lifetime on the trait

// Approach (2)
//      trait Args {
//          type Item: Clone,
//          type Iter: Iterator<Item=Self::Item>
//          fn get(&self, ...) -> Self::Item;
//          fn iter(&self, ...) -> Self::Iter;

// Scenarios:
// impl Args for &[T]
// Item: &T

// impl Args for Iterator<Item=T>
// Item: &T



////////////////////////////////////////////////////////////////////////////////
// PartialArgs
////////////////////////////////////////////////////////////////////////////////

/// Represents a partial set of arguments that can be split and 'popped' (in a stack like fashion).
pub struct PartialArgs<'a, A: Args<'a>> {
    args: &'a A,
    range: Range<usize>,
}

impl<'a, A: Args<'a> + Debug> Debug for PartialArgs<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    pub fn pop(&mut self) -> &'a A::Item {
        debug_assert!(self.len() > 1);

        let index = self.range.start;
        self.range.start += 1;

        self.args.get(index)
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

impl<'a, A: Args<'a>> IntoIterator for PartialArgs<'a, A> {
    type Item = &'a A::Item;
    type IntoIter = A::Iter;

    fn into_iter(self) -> Self::IntoIter {
        self.args.iter_range(self.range)
    }
}

////////////////////////////////////////////////////////////////////////////////
// SliceArgs
////////////////////////////////////////////////////////////////////////////////

/// A slice of arguments.
pub struct SliceArgs<'a, T>(&'a [T]);

impl<'a, T: Debug> Debug for SliceArgs<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SliceArgs").finish()
    }
}

impl<'a, T: Clone + Sync> Args<'a> for SliceArgs<'a, T> {
    type Item = T;
    type Iter = slice::Iter<'a, T>;

    #[inline]
    fn len(&self) -> usize { self.0.len() }

    #[inline]
    fn get(&'a self, index: usize) -> &'a Self::Item { &self.0[index] }

    #[inline]
    fn iter_range(&'a self, range: Range<usize>) -> Self::Iter { self.0[range].iter() }
}

////////////////////////////////////////////////////////////////////////////////
// IterCache
////////////////////////////////////////////////////////////////////////////////

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
    /// Create a new `IterCache` with `iter` as its base iterator.
    pub fn new(iter: I) -> IterCache<I> {
        let len = iter.len();
        let inner = IterCacheInner::new(iter);

        IterCache { len, inner: Arc::new(ShardedLock::new(inner)) }
    }

    /// Returns the length of this cache (equal to the length of its base iterator at construction).
    #[inline]
    pub fn len(&self) -> usize { self.len }

    /// Return's a reference to the item at position `index`, internally filling from the base
    /// iterator if required.
    pub fn get(&self, index: usize) -> &I::Item {
        unsafe {
            let inner = self.fill(&(index..index + 1));
            let ptr = inner.ptr.as_ptr().offset(index as isize);
            &*ptr
        }
    }

    /// Return's a slice reference of the items at positions within `range`, internally filling from
    /// the base iterator if required.
    pub fn slice(&self, range: Range<usize>) -> &[I::Item] {
        unsafe {
            let inner = self.fill(&range);
            let raw_ptr = inner.ptr.as_ptr().offset(range.start as isize);
            let ptr = NonNull::new_unchecked(raw_ptr);

            slice::from_raw_parts(ptr.as_ptr(), range.len())
        }
    }

    // TODO Iterators: maybe make them more lazy, fill from base as we yield items, storing the
    //      the owned values as we go?
    //      Would need an additional simple iter type that holds a ref to IterCache and tracks
    //      outstanding items with a range.

    /// Return's an iterator of references to items at positions within `range`, internally filling
    /// from the base iterator if required.
    #[inline]
    pub fn iter_range(&self, range: Range<usize>) -> slice::Iter<'_, I::Item> {
        self.slice(range).iter()
    }

    /// Return's an iterator of references to all of the items from the base iterator, internally
    /// filling from the base iterator if required.
    #[inline]
    pub fn iter(&self) -> slice::Iter<'_, I::Item> {
        self.slice(0..self.len).iter()
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

        // More elements from source iterator are required.
        { // Write lock.
            let mut inner = self.inner.write().unwrap();

            // Double check in case our requested range was filled in between dropping read lock
            // and acquiring the write lock.
            if !inner.is_filled(range) {
                inner.fill(range);
            }
        } // Write unlock.

        // Return the read lock to caller
        self.inner.read().unwrap()
    }
}

impl<'a, I> Args<'a> for IterCache<I>
where
    I: ExactSizeIterator + DoubleEndedIterator,
    I::Item: Clone + 'a,
{
    type Item = I::Item;
    type Iter = slice::Iter<'a, I::Item>;

    fn len(&self) -> usize {
        IterCache::len(self)
    }

    fn get(&'a self, index: usize) -> &'a Self::Item {
        IterCache::get(self, index)
    }

    fn iter_range(&'a self, range: Range<usize>) -> Self::Iter {
        IterCache::iter_range(self, range)
    }
}

// NOTE: This would be nice but it seems there's no feasible way to achieve this.
// impl<I, Idx> ops::Index<Idx> for IterCache<I>
// where
//     Idx: slice::SliceIndex<[I::Item]>
// {
//     type Output = Idx::Output;
//
//     fn index(&self, index: Idx) -> &Self::Output {
//         let inner = self.inner.read().unwrap();
//
//         // No way to do this without consuming the base iterator and filling every element.
//         // This would be misleading and not acceptable for an operation involving slice notation.
//         // The reason is the Index trait is opaque to us with regards numerical indexes. All we
//         // know is that it can select an item/sub-slice when given a slice. This means we cannot
//         // check and fill the necessary indexes, we would have to create the entire slice
//         // (meaning filling all items) even for a requested range that is a small fraction of the
//         // length.
//
//         // We can't implement SliceIndex ourselves as we would conflict with std's implementation
//         // due to orphan rules.
//     }
// }


////////////////////////////////////////////////////////////////////////////////
// IterCacheInner
////////////////////////////////////////////////////////////////////////////////

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

    unsafe fn fill(&mut self, range: &Range<usize>) {
        // Allocate memory if needed.
        let zero_sized = mem::size_of::<I::Item>() == 0;
        if !zero_sized && self.cap == 0 {
            let cap = self.iter.len();
            self.alloc(cap);
        }

        debug_assert!(range.end <= self.cap);

        // Find the first and last positions within the requested range that are not yet filled.
        let range_fill_start = cmp::max(range.start, self.empty.start);
        let range_fill_end = cmp::min(range.end, self.empty.end);

        // TODO delete this debug code.
        // if range_fill_start > range_fill_end {
        //     println!("range: {:?}", &range);
        //     println!("inner.empty: {:?}", &self.empty);
        //     panic!();
        // }

        // Count of items within requested range that need filling.
        debug_assert!(range_fill_start <= range_fill_end);
        let range_fill_count = range_fill_end - range_fill_start;

        // Now we can safely find the gap lengths, i.e. items not in the range but that would need
        // filling before the target range could be filled, for each fill direction.
        let f_gap = range_fill_start - self.empty.start;
        let b_gap = self.empty.end - range_fill_end;

        // If front-gap is empty or is smaller than the back gap after adjusting for a preference to
        // front-fill, then fill from the front.
        const F_WEIGHT: usize = 0;
        if f_gap == 0 || f_gap < (b_gap + F_WEIGHT) {
            self.fill_front(range_fill_count + f_gap);
        } else {
            self.fill_back(range_fill_count + b_gap);
        }
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

        let offset = (self.empty.end - 1) as isize;
        let ptr = self.ptr.as_ptr().offset(offset);

        for (i, val) in (0..count).zip(self.iter.by_ref().rev()) {
            ptr.offset(-(i as isize)).write(val);
        }

        self.empty.end -= count;
    }
}

impl<I: Iterator> Drop for IterCacheInner<I> {
    fn drop(&mut self) {
        debug_assert!(self.empty.end <= self.cap);

        unsafe {
            // Drop filled items at front.
            let count = self.empty.start;
            if count > 0 {
                let ptr = self.ptr.as_ptr();
                let slice = ptr::slice_from_raw_parts_mut(ptr, count);
                ptr::drop_in_place(slice);
            }

            // Drop filled items at back.
            let count = self.cap - self.empty.end;
            if count > 0 {
                let ptr = self.ptr.as_ptr().offset(self.empty.end as isize);
                let slice = ptr::slice_from_raw_parts_mut(ptr, count);
                ptr::drop_in_place(slice);
            }

            // Deallocate buffer memory.
            let elem_size = mem::size_of::<I::Item>();
            if self.cap > 0 && elem_size > 0 {
                let layout = Layout::array::<I::Item>(self.cap).unwrap();
                let ptr = self.ptr.as_ptr() as *mut u8;
                alloc::dealloc(ptr, layout);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use std::{thread, mem, iter};
    use std::time::Duration;
    use super::IterCache;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn iter_cache_zero_sized() {
        let zst_iter = (0..100).map(|_| ());

        // Test when whole of base consumed.
        test_iter_cache(zst_iter.clone(), |cache| {
            let collected = cache.iter().collect::<Vec<_>>();
            assert_eq!(collected.len(), 100);
        });

        // Test with single accesses towards each end.
        test_iter_cache(zst_iter.clone(), |cache| {
            // This is done purely for the drop checking.
            let _items = vec![cache.get(21), cache.get(79)];
        });
    }

    #[test]
    fn iter_cache_ranges() {
        const ITER_LEN: usize = 100;
        const RANGE_LEN: usize = 10;

        test_iter_cache(0..ITER_LEN, |cache| {
            let mut handles = vec![];

            for _ in 0..=8 {
                let cache = cache.clone();

                let handle = thread::spawn(move || {
                    for i in 0..(ITER_LEN - RANGE_LEN) {
                        // Front
                        let f_range = i..(i + RANGE_LEN);
                        let f_iter = cache.iter_range(f_range.clone());

                        let result = f_iter.map(|item| item.0).collect::<Vec<_>>();
                        let expected = f_range.collect::<Vec<_>>();

                        assert_eq!(&result, &expected);
                        thread::sleep(Duration::from_micros(50));

                        // Back
                        let base = (ITER_LEN - RANGE_LEN) - i;
                        let b_range = base..(base + RANGE_LEN);
                        let b_iter = cache.iter_range(b_range.clone());

                        let result = b_iter.map(|item| item.0).collect::<Vec<_>>();
                        let expected = b_range.collect::<Vec<_>>();

                        assert_eq!(&result, &expected);
                        thread::sleep(Duration::from_micros(200));
                    }
                });

                handles.push(handle);
            }

            for t in handles {
                t.join().unwrap();
            }
        });
    }

    ////////////////////////////////////////////////////////////////////////////
    // Test helpers
    ////////////////////////////////////////////////////////////////////////////

    static CREATE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone)]
    struct TestItem<T>(T);
    impl<T> TestItem<T> {
        fn new(val: T) -> Self {
            CREATE_COUNT.fetch_add(1, Ordering::SeqCst);
            TestItem(val)
        }
    }

    impl<T> Drop for TestItem<T> {
        fn drop(&mut self) {
            DROP_COUNT.fetch_add(1, Ordering::SeqCst);
        }
    }

    // Test wrapper base iterator type.
    type BaseIter<I> = iter::Map<I, fn(<I as Iterator>::Item) -> TestItem<<I as Iterator>::Item>>;

    // Create a base iterator for testing that counts yielded items.
    fn base_iter<I>(iter: I)
                    -> iter::Map<I, fn(I::Item) -> TestItem<I::Item>>
        where I: ExactSizeIterator + DoubleEndedIterator + Clone { iter.map(TestItem::new) }

    // Convenience function that wraps the 'iter' and its items with drop-count checking types.
    // The given `test_op` function is then invoked, passing the wrapped iterator for further
    // testing.
    fn test_iter_cache<I, F>(iter: I, test_op: F)
        where I: ExactSizeIterator + DoubleEndedIterator + Clone,
              F: Fn(&IterCache<BaseIter<I>>)
    {
        CREATE_COUNT.store(0, Ordering::SeqCst);
        DROP_COUNT.store(0, Ordering::SeqCst);

        let base = base_iter(iter);
        let cache = IterCache::new(base);

        // Perform the test operation.
        test_op(&cache);

        // Drop the cache and check test item drop counts
        mem::drop(cache);
        assert_eq!(CREATE_COUNT.load(Ordering::SeqCst), DROP_COUNT.load(Ordering::SeqCst));
    }

    // TODO currently unused, fix this (look at itertools test macros?).
    fn _iter_eq<I, J>(a: I, mut b: J) -> bool
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
