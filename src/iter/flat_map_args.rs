use rayon::iter::plumbing::*;
// use rayon::iter::
use rayon::iter::*;
use std::{iter, slice};
use std::ops::Range;
use std::marker::PhantomData;

// Ideas for how to split the args iterator:
//
//  * Make it parallel and splittable!
//
//  * Clone the whole IntoIter for every split and use front and back pointers
//
//  * Specify a 'collect' argument for args that indicates args should be collected

// We should focus on three variants:
//      - slice based,  possibly even parallel slices?
//      - standard iterator based, uses args length as splitting limit, clones args on split
//      - parallel iterator based for large numbers of args, args are indexed and thus splittable
//
//      Can we abstract these out with new trait(s)?


/// Represents a splittable collection of arguments, of a known length, to be given to a mapping
/// function.
// Notes:
//      * Clone:    When splitting the parallel iterator Args implementations will need to be
//                  cloned. Therefore Clone will need to be required on Args at
//                  IndexedParallelIterator implementations.
//                  Of course Args can be implemented on a reference
//      * Should Args extend IntoIterator? Or should we just have an iter_range method?

// Args: a collection of 'arguments' with known length, that can be split into left and right halves
//       at a given index.
// MapArgs: owns map_op and args, producers hold a ref to this in order to apply to items
// MapArgsIter: iterates over all mappings of map_op(item, arg).

pub trait Args: Sized {
    type Iter: Iterator<Item=Self::Item>;
    type Item: Clone;

    fn len(&self) -> usize;
    fn iter_range(&self, range: Range<usize>) -> Self::Iter;
    fn iter(&self) -> Self::Iter { self.iter_range(0..self.len()) }
}

impl<'a, T> Args for &'a [T] {
    type Iter = slice::Iter<'a, T>;
    type Item = &'a T;

    fn len(&self) -> usize { <[T]>::len(self) }
    fn iter_range(&self, range: Range<usize>) -> Self::Iter { self[range].iter() }
    fn iter(&self) -> Self::Iter { <[T]>::iter(self) }
}

// Owns the mapping operation closure and the arguments. The main purpose of this type is as a
// convenient (owning) container for the mapping closure and the arguments, and to reduce
// type bounds boilerplate throughout the rayon trait impls.
pub struct MapArgs<F, T, A, R>
    where F: Fn(T, A::Item) -> R,
          A: Args,
          T: Clone,
          R: Send,
{
    map_op: F,
    args: A,
    _marker: PhantomData<(T, R)>
}

impl<F, T, A, R> MapArgs<F, T, A, R>
    where F: Fn(T, A::Item) -> R + Clone,
          A: Args,
          T: Clone,
          R: Send,
{
    fn map_all<'f>(&'f self, item: T) -> MapArgsIter<'f, F, T, A> {
        let args_iter = self.args.iter();
        MapArgsIter { map_op: &self.map_op, args_iter, item}
    }

    fn map_range<'f>(&'f self, item: T, range: Range<usize>) -> MapArgsIter<'f, F, T, A> {
        let args_iter = self.args.iter_range(range);
        MapArgsIter { map_op: &self.map_op, args_iter, item}
    }
}

pub struct MapArgsIter<'f, F, T, A>
    where A: Args,
          T: Clone,
{
    map_op: &'f F,
    args_iter: A::Iter,
    item: T,
}

impl<'f, F, T, A, R> Iterator for MapArgsIter<'f, F, T, A>
    where F: Fn(T, A::Item) -> R,
          A: Args,
          T: Clone,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        self.args_iter.next()
            .map(|arg| (self.map_op)(self.item.clone(), arg))
    }
}


trait Split<T>
    where Self: Sized,
{
    fn len(&self) -> usize;

    fn split_at(self, index: usize) -> (Self, Self);
}

impl<T: Clone> Split<T> for Option<(T, Range<usize>)>{
    fn len(&self) -> usize {
        match self {
            Some((_, range)) => range.len(),
            None => 0,
        }
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        match self {
            Some((item, range)) => {
                debug_assert!(0 < index && index <= range.end );

                let (l_range, r_range) = (range.start..index, index..range.end);

                // Split into left and right sides, avoiding unnecessary cloning.
                if r_range.is_empty() {
                    (Some((item, l_range)), None)
                } else if l_range.is_empty() {
                    (None, Some((item, r_range)))
                } else {
                    (Some((item.clone(), l_range)), Some((item, r_range)))
                }
            }
            None => {
                debug_assert!(index == 0);
                (None, None)
            }
        }
    }
}

// TODO a type that wraps Fn and Args, maybe call it MapArgs?
//      benefits:
//          * Reduces generic boilerplate on types, impls,  and functions.
//          * Improves ergonomics (less repetition, can provide iterator mapping item to


/// `FlatMapExact` is an iterator...
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
#[derive(Clone, Debug)]
pub struct FlatMapExact<I, F, A> {
    base: I,
    map_op: F,
    args: A,
}

pub fn flat_map_exact<I, F, A>(base: I, map_op: F, args: A) -> FlatMapExact<I, F, A>
    where I: ParallelIterator,
          A: Args, { FlatMapExact { base, map_op, args } }

impl<I, F, A, R> ParallelIterator for FlatMapExact<I, F, A>
    where I: ParallelIterator,
          I::Item: Clone + Send,
          A: Args + Sync + Send,
          F: Fn(I::Item, A::Item) -> R + Sync + Send,
          R: Send,
{
    type Item = R;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
        where
            C: UnindexedConsumer<Self::Item>,
    {
        let consumer1 = FlatMapExactConsumer::new(consumer, &self.map_op, &self.args);
        self.base.drive_unindexed(consumer1)
    }

    fn opt_len(&self) -> Option<usize> {
        self.base.opt_len()
            .and_then(|len| len.checked_mul(self.args.len()))
    }
}


impl<I, F, A, R> IndexedParallelIterator for FlatMapExact<I, F, A>
    where I: PopParallelIterator,
          I::Item: Clone + Send,
          F: Fn(I::Item, A::Item) -> R + Sync + Send,
          A: Args + Sync + Send,
          R: Send,
{
    fn drive<C>(self, consumer: C) -> C::Result
        where C: Consumer<Self::Item>,
    {
        let consumer1 = FlatMapExactConsumer::new(consumer, &self.map_op, &self.args);
        self.base.drive(consumer1)
    }

    fn len(&self) -> usize {
        self.base.len()
            .checked_mul(self.args.len())
            .expect("overflow")
    }

    // Main scenarios:

    // There are three main kinds of indexed iterators with regards to pop operations:
    //      1) Those that do not support pop operations but of course may be 'below' one that does
    //      2) Those that support pop operations but don't require them in order to be indexed
    //      3) Those that require pop operations in order to be indexed

    //  with_producer is called on non-pop-iterator by non-pop-iterator
    //  with_producer is called on pop-iterator by non-pop-iterator
    //  with_producer is called on pop-iterator by pop-iterator


    fn with_producer<CB>(self, callback: CB) -> CB::Output
        where CB: ProducerCallback<Self::Item>,
    {
        let len = self.len();
        return self.base.with_pop_producer(Callback {
            callback,
            map_op: self.map_op,
            args: self.args,
            len,
        });

        struct Callback<CB, F, A> {
            callback: CB,
            map_op: F,
            args: A,
            len: usize
        }

        // TODO try using the outer impl type parameters
        impl<F, T, A, R, CB> PopProducerCallback<T> for Callback<CB, F, A>
            where CB: ProducerCallback<R>,
                  F: Fn(T, A::Item) -> R + Sync,
                  T: Clone + Send,
                  A: Args + Sync,
                  R: Send,
        {
            type Output = CB::Output;

            fn pop_callback<P>(self, base: P) -> CB::Output
                where P: PopProducer<Item=T>,
            {
                let producer = FlatMapExactProducer::new(base, &self.map_op, &self.args, None, None, self.len);
                self.callback.callback(producer)
            }
        }
    }
}

impl<I, F, A, R> PopParallelIterator for FlatMapExact<I, F, A>
    where I: PopParallelIterator,
          I::Item: Clone + Send,
          F: Fn(I::Item, A::Item) -> R + Sync + Send,
          A: Args + Sync + Send,
          R: Send,
{
    fn with_pop_producer<CB>(self, callback: CB) -> CB::Output
        where CB: PopProducerCallback<Self::Item>,
    {
        let len = self.len();
        return self.base.with_pop_producer(Callback {
            callback,
            map_op: self.map_op,
            args: self.args,
            len,
        });

        struct Callback<CB, F, A> {
            callback: CB,
            map_op: F,
            args: A,
            len: usize,
        }

        impl<F, T, A, R, CB> PopProducerCallback<T> for Callback<CB, F, A>
            where CB: PopProducerCallback<R>,
                  F: Fn(T, A::Item) -> R + Sync,
                  T: Clone + Send,
                  A: Args + Sync,
                  R: Send,
        {
            type Output = CB::Output;

            fn pop_callback<P>(self, base: P) -> CB::Output
                where
                    P: PopProducer<Item=T>,
            {
                let producer = FlatMapExactProducer::new(base, &self.map_op, &self.args, None, None, self.len);
                self.callback.pop_callback(producer)
            }
        }
    }
}


struct FlatMapExactProducer<'f, P, F, A>
    where P: PopProducer,
          A: Args,
{
    base: P,
    map_op: &'f F,
    args: &'f A,
    front: Option<(P::Item, Range<usize>)>,
    back: Option<(P::Item, Range<usize>)>,
    len: usize,
}


// impl<'f, P, F, A, R> FlatMapExactProducer<'f, P, F, A>
impl<'f, P, F, A, R> FlatMapExactProducer<'f, P, F, A>
    where P: PopProducer,
          A: Args,
          F: Fn(P::Item, A::Item) -> R + Sync,
          R: Send,
{
    fn new(
        base: P,
        map_op: &'f F,
        args: &'f A,
        front: Option<(P::Item, Range<usize>)>,
        back: Option<(P::Item, Range<usize>)>,
        len: usize) -> Self
    {
        FlatMapExactProducer { base, map_op, args, front, back, len }
    }

    // fn front_len(&self) -> usize {
    //     let front_len = if let Some((_, start)) = self.front {
    //         debug_asset!(start <= self.args.len());
    //         self.args.len() - start
    //     } else {
    //         0
    //     };
    // }
    //
    // fn back_len(&self) -> usize {
    //     let back_len = if let Some((_, end)) = self.back {
    //         debug_asset!(end <= self.args.len());
    //         end
    //     } else {
    //         0
    //     };
    // }
}

impl<'f, P, F, A, R> Producer for FlatMapExactProducer<'f, P, F, A>
    where P: PopProducer,
          P::Item: Clone + Send,
          F: Fn(P::Item, A::Item) -> R + Sync,
          A: Args + Sync,
          R: Send,
{
    type Item = F::Output;
    type IntoIter = FlatMapExactIter<'f, P::IntoIter, F, A>;

    fn into_iter(self) -> Self::IntoIter {
        FlatMapExactIter {
            base: self.base.into_iter().fuse(),
            map_op: self.map_op,
            args: self.args,
            front: self.front,
            back: self.back,
        }
    }

    // fn min_len(&self) -> usize {
    //     // TODO not sure if we should change this to depend on doubling factor and front/back
    //     self.base.min_len()
    // }
    // fn max_len(&self) -> usize {
    //     // TODO not sure if we should change this to depend on doubling factor and front/back
    //     self.base.max_len()
    // }

    fn split_at(self, index: usize) -> (Self, Self) {
        // Working example:

        // Working example:
        //
        // Initial logical view of un-split iterator:
        // logical: [f(1, a1), f(1, a2), f(2, a1), f(2, a2), f(3, a1), f(3, a2),]
        // state:   () [1, 2, 3] ()

        // Subsequent splits:
        // logical: [f(1, a1), f(1, a2), f(2, a1)]  [f(2, a2), f(3, a1), f(3, a2),]
        // state:   () [1] (2, 0..1)                (2, 1..2) [3] ()

        // logical: [f(1, a1)]       [f(1, a2), f(2, a1)]  [f(2, a2), f(3, a1), f(3, a2),]
        // state:   (1, 0..1) [] ()  (2, 1..2) [3] ()


        // Implementation notes:
        //
        // Main cases:
        //  * split occurs before base:
        //
        //  * split occurs within base:
        //
        //  * split occurs after base:

        // idea:
        //  * Have Split be an enum: Split(Empty, )

        debug_assert!(index <= self.len);

        // TODO see if there's a way to avoid unwrapping front and back here only to do it again
        //      further down.
        let front_len = self.front.len();
        let back_len = self.back.len();

        let r_len = self.len - index;
        let l_len = self.len - r_len;

        // TODO optimise reuse of of this producer (self) to aid return value optimisation.

        if index <= front_len {
            // Split occurs at the front, left base will be empty.
            let (l_base, r_base) = self.base.split_at(0);
            let (l_front, r_front) = self.front.split_at(index);

            (
                FlatMapExactProducer::new(l_base, self.map_op, self.args.clone(), l_front, None, l_len),
                FlatMapExactProducer::new(r_base, self.map_op, self.args, r_front, self.back, r_len),
            )

        } else {
            // First logical index that is past the base.
            let base_end = self.len - back_len;

            if index <= base_end {
                // Split occurs 'within' the base producer.

                // If there is a 'split' base item (i.e. both left and right hand bases need to
                // produce an item based on a single base item), then we pop it from the right hand
                // base to allow us to populate it as left's last item and right's first item.

                // If there is an item to be split it will be the first item in the split base's
                // right hand side.

                let args_len = self.args.len();

                // How many logical items 'into' the base is the index.
                let index_base_offset = index - front_len;

                let base_split = index_base_offset / args_len;
                let (l_base, mut r_base) = self.base.split_at(base_split);

                // Check for split across an item.
                let arg_index = index_base_offset % args_len;
                let (l_back, r_front) = match arg_index {
                    0 => (None, None),
                    _ => {
                        let item = r_base.pop();
                        (Some((item.clone(), 0..arg_index)), Some((item, arg_index..args_len)))
                    }
                };

                (
                    FlatMapExactProducer::new(l_base, self.map_op, self.args.clone(), self.front, l_back, l_len),
                    FlatMapExactProducer::new(r_base, self.map_op, self.args, r_front, self.back, r_len),
                )

            } else {
                debug_assert!(index == self.len);

                // Split occurs within self.back, right base will be empty.
                let base_len = base_end - front_len;

                let (l_base, r_base) = self.base.split_at(0);
                let (l_back, r_front) = self.back.split_at(index);

                (
                    FlatMapExactProducer::new(l_base, self.map_op, self.args.clone(), self.front, l_back, l_len),
                    FlatMapExactProducer::new(r_base, self.map_op, self.args, r_front, None, r_len),
                )

            }
        }
        //
        //
        //
        //
        // // debug_assert!(index <= self.len);
        //
        // // Split the base producer at the correct index. If there is a 'split' item it will be
        // // the first item on the right hand producer.
        //
        //
        // // How many items in after front, i.e. from the index where base items start being used.
        // let base_offset = if index > front_len { index - front_len } else { index };
        //
        // }
        //
        // let base_index = if index > front_len
        //
        // let base_offset = if index > 0 && self.front.is_some() { index - 1 } else { index };
        // let base_index = base_offset / 2;
        // let (left_base, mut right_base) = self.base.split_at(base_index);
        //
        // // If there is a 'split' base item (i.e. both left and right hand bases need to produce an
        // // item based on a single base item), then pop it from the right hand base to allow us to
        // // populate it as left's last item and right's first item.
        // let split = base_offset % 2 != 0;
        // let item = if split { right_base.pop() } else { None };
        //
        // let left = FlatMapExactProducer {
        //     base: left_base,
        //     map_op: self.map_op,
        //     front: self.front,
        //     back: item.clone(),
        //     // len: index,
        // };
        //
        // let right = FlatMapExactProducer {
        //     base: right_base,
        //     map_op: self.map_op,
        //     front: item,
        //     back: self.back,
        //     // len: index,
        // };
        //
        // (left, right)
    }

    fn fold_with<G>(self, folder: G) -> G
        where
            G: Folder<Self::Item>,
    {

        let folder1 = FlatMapExactFolder {
            base: folder,
            map_op: self.map_op,
            args: self.args,
            front: self.front,
            back: self.back,
        };
        // self.base.fold_with(folder1).base

        // TODO investigate if this is correct here: it would appear that complete() is the place
        //      to consume the rear item, if any, but it appears that the base consumer may assert
        //      the correct length before this gets called.
        // Consume the last item before any correctness assertions are performed.
        let mut folder = self.base.fold_with(folder1);

        if !folder.base.full() {
            if let Some((item, range)) = folder.back.take() {
                let map_op = folder.map_op;
                let iter = folder.args.iter_range(range)
                    .map(|arg| map_op(item.clone(), arg));

                folder.base = folder.base.consume_iter(iter);
            }
        }

        folder.base
    }
}

impl<'f, P, F, A, R> PopProducer for FlatMapExactProducer<'f, P, F, A>
    where P: PopProducer,
          P::Item: Clone + Send,
          F: Fn(P::Item, A::Item) -> R + Sync,
          A: Args + Sync,
          R: Send,
{
    fn try_pop(&mut self) -> Option<Self::Item> {
        // if let Some(item) = self.front.take() {
        //     Some((self.map_op)(item))
        // } else if let Some(item) = self.base.pop() {
        //     self.front = Some(item.clone());
        //     Some(item)
        // } else {
        //     self.back.take()
        // }
        unimplemented!()
    }
}

struct FlatMapExactIter<'f, I, F, A>
    where I: Iterator,
{
    base: iter::Fuse<I>,
    map_op: &'f F,
    args: &'f A,
    front: Option<(I::Item, Range<usize>)>,
    back: Option<(I::Item, Range<usize>)>,
}

impl<'f, I, F, A, R> Iterator for FlatMapExactIter<'f, I, F, A>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, A::Item) -> R,
        A: Args,
{
    type Item = F::Output;

    fn next(&mut self) -> Option<Self::Item> {
        // // If there is a front item then return its mapped value.
        // if let Some(item) = self.front.take() {
        //     return Some((self.map_op)(item));
        // }
        //
        // // Otherwise, if there is an item from the base, store a clone at the front and return it.
        // if let Some(item) = self.base.next() {
        //     self.front = Some(item.clone());
        //     return Some(item);
        // }
        //
        // // Otherwise, return any item that may be at the back.
        // self.back.take()
        unimplemented!()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'f, I, F, A, R> DoubleEndedIterator for FlatMapExactIter<'f, I, F, A>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, A::Item) -> R,
        A: Args,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        // // If there is a back item then return it.
        // if self.back.is_some() { return self.back.take(); }
        //
        // // Otherwise, if there is an item from the base, store a clone at the back and return its
        // // mapped value.
        // if let Some(item) = self.base.next_back() {
        //     self.back = Some(item.clone());
        //     return Some((self.map_op)(item));
        // }
        //
        // // Otherwise, return the mapped value of any item that may be at the front.
        // self.back.take().map(self.map_op)
        unimplemented!()
    }
}

impl<'f, I, F, A, R> ExactSizeIterator for FlatMapExactIter<'f, I, F, A>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, A::Item) -> R,
        A: Args,
{
    fn len(&self) -> usize {
        self.base.len().checked_mul(self.args.len())
            .and_then(|len| len.checked_add(self.front.len()))
            .and_then(|len| len.checked_add(self.back.len()))
            .expect("overflow")
    }
}

struct FlatMapExactConsumer<'f, C, F, A> {
    base: C,
    map_op:  &'f F,
    args: &'f A,
}

impl<'f, C, F, A> FlatMapExactConsumer<'f, C, F, A>{
    fn new(base: C, map_op: &'f F, args: &'f A) -> Self { FlatMapExactConsumer { base, map_op, args } }
}

impl<'f, C, F, A, T, R> Consumer<T> for FlatMapExactConsumer<'f, C, F, A>
    where C: Consumer<F::Output>,
          F: Fn(T, A::Item) -> R + Sync,
          T: Clone + Send,
          A: Args + Sync,
{
    type Folder = FlatMapExactFolder<'f, C::Folder, F, T, A>;
    type Reducer = C::Reducer;
    type Result = C::Result;

    fn split_at(mut self, index: usize) -> (Self, Self, Self::Reducer) {
        // We'll always feed twice as many items to the base consumer.
        let base_index = index.checked_mul(2).expect("overflow");
        let (left_base, right_base, reducer) = self.base.split_at(base_index);

        let right = FlatMapExactConsumer {
            base: right_base,
            map_op: self.map_op,
            args: self.args.clone(),
        };

        self.base = left_base;
        (self, right, reducer)
    }

    fn into_folder(self) -> Self::Folder {

        FlatMapExactFolder {
            base: self.base.into_folder(),
            map_op: self.map_op,
            args: self.args,
            front: None,
            back: None,
        }
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

impl<'f, T, C, F, A, R> UnindexedConsumer<T> for FlatMapExactConsumer<'f, C, F, A>
    where C: UnindexedConsumer<F::Output>,
          F: Fn(T, A::Item) -> R + Sync,
          T: Clone + Send,
          A: Args + Sync,
          R: Send,
{
    fn split_off_left(&self) -> Self {
        let left = FlatMapExactConsumer {
            base: self.base.split_off_left(),
            map_op: self.map_op,
            args: self.args.clone(),
        };
        left
    }

    fn to_reducer(&self) -> Self::Reducer {
        self.base.to_reducer()
    }
}

struct FlatMapExactFolder<'f, C, F, T, A> {
    base: C,
    map_op: &'f F,
    args: &'f A,
    front: Option<(T, Range<usize>)>,
    // TODO this is possibly redundant
    back: Option<(T, Range<usize>)>,
}

impl<'f, C, F, T, A, R> Folder<T> for FlatMapExactFolder<'f, C, F, T, A>
    where
        C: Folder<F::Output>,
        F: Fn(T, A::Item) -> R,
        T: Clone,
        A: Args,
{
    type Result = C::Result;

    fn consume(mut self, item: T) -> Self {
        // // Feeds up to 3 items to base folder: map_op(front), item, map_op(item)
        //
        // if let Some(item) = self.front.take() {
        //     self.base = self.base.consume((self.map_op)(item));
        //     if self.base.full() { return self; }
        // }
        //
        // self.base = self.base.consume(item.clone());
        //
        // if self.base.full() { return self; }
        // self.base = self.base.consume((self.map_op)(item));
        //
        // self
        unimplemented!()
    }

    fn consume_iter<I>(mut self, iter: I) -> Self
        where I: IntoIterator<Item=T>,
    {
        // if let Some(item) = self.front.take() {
        //     let mapped = (self.map_op)(item);
        //     self.base = self.base.consume(mapped);
        //     if self.base.full() { return self; }
        // }
        //
        // for item in iter.into_iter() {
        //     self.base = self.base.consume(item.clone());
        //     if self.base.full() { return self; }
        //
        //     let mapped = (self.map_op)(item);
        //     self.base = self.base.consume(mapped);
        //     if self.base.full() { return self; }
        // }
        //
        // // TODO investigate if this is correct here: it would appear that complete() is the place
        // //      to consume the rear item, if any, but it appears that the base consumer may assert
        // //      the correct length before this gets called.
        // //      For now the operation is done at <MapInterleaved as Producer>::with_folder()
        // // if let Some(item) = self.back.take() {
        // //     self.base = self.base.consume(item);
        // // }
        //
        // self
        unimplemented!()
    }

    fn complete(self) -> C::Result {
        self.base.complete()
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}
