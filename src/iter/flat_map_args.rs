use super::plumbing::*;
use super::*;
use std::iter;
use std::ops::Range;

// TODO future ideas:
//      * version with parallel arguments (uses poppable producer)

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

pub(crate) fn flat_map_exact<I, F, A>(base: I, map_op: F, args: A) -> FlatMapExact<I, F, A>
    where I: ParallelIterator,
          A: for<'a> Args<'a>, { FlatMapExact { base, map_op, args } }

impl<I, F, A, R> ParallelIterator for FlatMapExact<I, F, A>
    where I: ParallelIterator,
          I::Item: Clone + Send,
          A: for<'a> Args<'a>,
          F: for<'a> Fn(I::Item, &'a <A as Args<'a>>::Item) -> R + Sync + Send,
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
          F: for<'a> Fn(I::Item, &'a <A as Args<'a>>::Item) -> R + Sync + Send,
          A: for<'a> Args<'a>,
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

        impl<F, T, A, R, CB> PopProducerCallback<T> for Callback<CB, F, A>
            where CB: ProducerCallback<R>,
                  F: for<'a> Fn(T, &'a <A as Args<'a>>::Item) -> R + Sync,
                  T: Clone + Send,
                  A: for<'a> Args<'a>,
                  R: Send,
        {
            type Output = CB::Output;

            fn pop_callback<P>(self, base: P) -> CB::Output
                where P: PopProducer<Item=T>,
            {
                let (front, back) = (SplitItem::Empty, SplitItem::Empty);
                let producer = FlatMapExactProducer::new(base, &self.map_op, &self.args, front, back, self.len);
                self.callback.callback(producer)
            }
        }
    }
}

impl<I, F, A, R> PopParallelIterator for FlatMapExact<I, F, A>
    where I: PopParallelIterator,
          I::Item: Clone + Send,
          F: for<'a> Fn(I::Item, &'a <A as Args<'a>>::Item) -> R + Sync + Send,
          A: for<'a> Args<'a>,
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
                  F: for<'a> Fn(T, &'a <A as Args<'a>>::Item) -> R + Sync,
                  T: Clone + Send,
                  A: for<'a> Args<'a>,
                  R: Send,
        {
            type Output = CB::Output;

            fn pop_callback<P>(self, base: P) -> CB::Output
                where
                    P: PopProducer<Item=T>,
            {
                let (front, back) = (SplitItem::Empty, SplitItem::Empty);
                let producer = FlatMapExactProducer::new(base, &self.map_op, &self.args, front, back, self.len);
                self.callback.pop_callback(producer)
            }
        }
    }
}


struct FlatMapExactProducer<'a, P, F, A>
    where P: PopProducer,
          A: Args<'a>,
{
    base: P,
    map_op: &'a F,
    args: &'a A,
    front: SplitItem<P::Item>,
    back: SplitItem<P::Item>,
    len: usize,
}

impl<'a, P, F, A, R> FlatMapExactProducer<'a, P, F, A>
    where P: PopProducer,
          A: Args<'a>,
          F: Fn(P::Item, &'a A::Item) -> R + Sync,
          R: Send,
{
    fn new(
        base: P,
        map_op: &'a F,
        args: &'a A,
        front: SplitItem<P::Item>,
        back: SplitItem<P::Item>,
        len: usize) -> Self
    {
        FlatMapExactProducer { base, map_op, args, front, back, len }
    }
}

impl<'a, P, F, A, R> Producer for FlatMapExactProducer<'a, P, F, A>
    where P: PopProducer,
          P::Item: Clone + Send,
          F: Fn(P::Item, &'a A::Item) -> R + Sync,
          A: Args<'a>,
          R: Send,
{
    type Item = F::Output;
    type IntoIter = FlatMapExactIter<'a, P::IntoIter, F, A>;

    fn into_iter(self) -> Self::IntoIter {
        FlatMapExactIter {
            base: self.base.into_iter().fuse(),
            map_op: self.map_op,
            args: self.args,
            front: self.front,
            back: self.back,
        }
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        // Implementation notes:
        //
        // Main cases:
        //  * split occurs before base:
        //  * split occurs within base:
        //  * split occurs after base:

        // Working example:
        //
        // Initialised producer:
        // logical: [f(1, a1), f(1, a2), f(2, a1), f(2, a2), f(3, a1), f(3, a2),]
        // state:   () [1, 2, 3] ()
        //
        // after split_at(3):
        // logical: [f(1, a1), f(1, a2), f(2, a1)]  [f(2, a2), f(3, a1), f(3, a2),]
        // state:   () [1] (2, 0..1)                (2, 1..2) [3] ()
        //
        // after left.split_at(1) and right.split_at(5)
        // logical: [f(1, a1)]       [f(1, a2), f(2, a1)]       [f(2, a2), f(3, a1)]    [f(3, a2),]
        // state:   (1, 0..1) [] ()  (1, 1..2) [] (2, 0..1)     (2, 1..2) [] (3, 0..1)  (3, 1..2) [] ()

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
            let l_back = SplitItem::Empty;

            (
                FlatMapExactProducer::new(l_base, self.map_op, self.args, l_front, l_back, l_len),
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

                let args_len = self.args.len();

                // How many logical items 'into' the base is the index.
                let index_base_offset = index - front_len;

                let base_split = index_base_offset / args_len;
                let (l_base, mut r_base) = self.base.split_at(base_split);

                // Check for split across an item.
                let arg_index = index_base_offset % args_len;
                let (l_back, r_front) = match arg_index {
                    0 => (SplitItem::Empty, SplitItem::Empty),
                    _ => {
                        let item = r_base.pop();
                        (
                            SplitItem::Some(item.clone(), 0..arg_index),
                            SplitItem::Some(item, arg_index..args_len)
                        )
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
                let r_back = SplitItem::Empty;

                (
                    FlatMapExactProducer::new(l_base, self.map_op, self.args.clone(), self.front, l_back, l_len),
                    FlatMapExactProducer::new(r_base, self.map_op, self.args, r_front, r_back, r_len),
                )

            }
        }
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
            if let SplitItem::Some(item, range) = folder.back {
                let map_op = folder.map_op;
                let iter = folder.args.iter_range(range)
                    .map(|arg| map_op(item.clone(), arg));

                folder.base = folder.base.consume_iter(iter);
            }
        }

        folder.base
    }
}

impl<'a, P, F, A, R> PopProducer for FlatMapExactProducer<'a, P, F, A>
    where P: PopProducer,
          P::Item: Clone + Send,
          F: Fn(P::Item, &'a A::Item) -> R + Sync,
          A: Args<'a>,
          // A::Item: Clone,
          R: Send,
{
    fn try_pop(&mut self) -> Option<Self::Item> {
        // Try the front.
        if let Some(mapped) = self.front.pop_map(self.map_op, self.args) {
            return Some(mapped);
        }

        // Front is empty try base producer.
        if let Some(item) = self.base.try_pop() {
            // We have to check for empty args list here...
            if self.args.len() == 0 { return None; }

            // Since we've popped from the base we now need a 'partial' item at the front.
            self.front = SplitItem::Some(item.clone(), 1..self.args.len());

            let mapped = (self.map_op)(item, self.args.get(0));
            return Some(mapped);
        }

        // Base is empty, try the back.
        if let Some(mapped) = self.back.pop_map(self.map_op, self.args) {
            Some(mapped)
        } else {
            // Producer is empty.
            None
        }

    }
}

struct FlatMapExactIter<'f, I, F, A>
    where I: Iterator,
{
    base: iter::Fuse<I>,
    map_op: &'f F,
    args: &'f A,
    front: SplitItem<I::Item>,
    back: SplitItem<I::Item>,
}

impl<'a, I, F, A, R> Iterator for FlatMapExactIter<'a, I, F, A>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: Args<'a>,
{
    type Item = F::Output;

    fn next(&mut self) -> Option<Self::Item> {
        // Try the front.
        if let Some(mapped) = self.front.pop_map(self.map_op, self.args) {
            return Some(mapped);
        }

        // Front is empty try base producer.
        if let Some(item) = self.base.try_pop() {
            // We have to check for empty args list here...
            if self.args.len() == 0 { return None; }

            // Since we've popped from the base we now need a 'partial' item at the front.
            self.front = SplitItem::Some(item.clone(), 1..self.args.len());

            let mapped = (self.map_op)(item, self.args.get(0));
            return Some(mapped);
        }

        // Base is empty, try the back.
        if let Some(mapped) = self.back.pop_map(self.map_op, self.args) {
            Some(mapped)
        } else {
            // Producer is empty.
            None
        }

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

impl<'a, I, F, A, R> DoubleEndedIterator for FlatMapExactIter<'a, I, F, A>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: Args<'a>,
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

impl<'a, I, F, A, R> ExactSizeIterator for FlatMapExactIter<'a, I, F, A>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: Args<'a>,
{
    fn len(&self) -> usize {
        self.base.len().checked_mul(self.args.len())
            .and_then(|len| len.checked_add(self.front.len()))
            .and_then(|len| len.checked_add(self.back.len()))
            .expect("overflow")
    }
}

struct FlatMapExactConsumer<'a, C, F, A> {
    base: C,
    map_op:  &'a F,
    args: &'a A,
}

impl<'f, C, F, A> FlatMapExactConsumer<'f, C, F, A>{
    fn new(base: C, map_op: &'f F, args: &'f A) -> Self { FlatMapExactConsumer { base, map_op, args } }
}

impl<'a, C, F, A, T, R> Consumer<T> for FlatMapExactConsumer<'a, C, F, A>
    where C: Consumer<F::Output>,
          F: Fn(T, &'a A::Item) -> R + Sync,
          // T: Clone,
          A: Args<'a>,
{
    type Folder = FlatMapExactFolder<'a, C::Folder, F, T, A>;
    type Reducer = C::Reducer;
    type Result = C::Result;

    fn split_at(mut self, index: usize) -> (Self, Self, Self::Reducer) {
        // We'll always feed twice as many items to the base consumer.
        let base_index = index.checked_mul(2).expect("overflow");
        let (left_base, right_base, reducer) = self.base.split_at(base_index);

        let right = FlatMapExactConsumer {
            base: right_base,
            map_op: self.map_op,
            args: self.args,
        };

        self.base = left_base;
        (self, right, reducer)
    }

    fn into_folder(self) -> Self::Folder {

        FlatMapExactFolder {
            base: self.base.into_folder(),
            map_op: self.map_op,
            args: self.args,
            front: SplitItem::Empty,
            back: SplitItem::Empty,
        }
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

impl<'a, T, C, F, A, R> UnindexedConsumer<T> for FlatMapExactConsumer<'a, C, F, A>
    where C: UnindexedConsumer<F::Output>,
          F: Fn(T, &'a A::Item) -> R + Sync,
          T: Clone + Send,
          A: Args<'a>,
          R: Send,
{
    fn split_off_left(&self) -> Self {
        let left = FlatMapExactConsumer {
            base: self.base.split_off_left(),
            map_op: self.map_op,
            args: self.args,
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
    front: SplitItem<T>,
    // TODO this is possibly redundant
    back: SplitItem<T>,
}

impl<'a, C, F, T, A, R> Folder<T> for FlatMapExactFolder<'a, C, F, T, A>
    where
        C: Folder<F::Output>,
        F: Fn(T, &'a A::Item) -> R,
        // T: Clone,
        A: Args<'a>,
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

////////////////////////////////////////////////////////////////////////////////////////
// Dumping ground
////////////////////////////////////////////////////////////////////////////////////////

// // Owns the mapping operation closure and the arguments. The main purpose of this type is as a
// // convenient (owning) container for the mapping closure and the arguments, and to reduce
// // type bounds boilerplate throughout the rayon trait impls.
// pub struct MapArgs<F, T, A, R>
//     where F: Fn(T, A::Item) -> R,
//           A: Args,
//           T: Clone,
//           R: Send,
// {
//     map_op: F,
//     args: A,
//     _marker: PhantomData<(T, R)>
// }
//
// impl<F, T, A, R> MapArgs<F, T, A, R>
//     where F: Fn(T, A::Item) -> R + Clone,
//           A: Args,
//           T: Clone,
//           R: Send,
// {
//     fn map_all<'f>(&'f self, item: T) -> MapArgsIter<'f, F, T, A> {
//         let args_iter = self.args.iter();
//         MapArgsIter { map_op: &self.map_op, args_iter, item}
//     }
//
//     fn map_range<'f>(&'f self, item: T, range: Range<usize>) -> MapArgsIter<'f, F, T, A> {
//         let args_iter = self.args.iter_range(range);
//         MapArgsIter { map_op: &self.map_op, args_iter, item}
//     }
// }
//
// pub struct MapArgsIter<'f, F, T, A>
//     where A: Args,
//           T: Clone,
// {
//     map_op: &'f F,
//     args_iter: A::Iter,
//     item: T,
// }
//
// impl<'f, F, T, A, R> Iterator for MapArgsIter<'f, F, T, A>
//     where F: Fn(T, A::Item) -> R,
//           A: Args,
//           T: Clone,
// {
//     type Item = R;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         self.args_iter.next()
//             .map(|arg| (self.map_op)(self.item.clone(), arg))
//     }
// }

enum SplitItem<T> {
    Some(T, Range<usize>),
    Empty,
}

impl<T: Clone> SplitItem<T> {
    fn len(&self) -> usize {
        match &self {
            SplitItem::Some(_, range) => range.len(),
            SplitItem::Empty => 0,
        }
    }

    fn split_at(mut self, index: usize) -> (Self, Self) {
        match &mut self {
            SplitItem::Some(item, range) => {
                debug_assert!(index <= range.end );

                match index {
                    0 => (self, SplitItem::Empty),
                    i if i == range.end => (SplitItem::Empty, self),
                    _ => {
                        let right = SplitItem::Some(item.clone(), index..range.end);
                        *range = range.start..index;
                        (self, right)
                    },
                }
            }
            SplitItem::Empty => {
                // TODO does it even make sense to allow splitting of Empty, should we panic here?
                debug_assert!(index == 0);
                (self, SplitItem::Empty)
            }
        }
    }

    fn pop_map<'a, F, A, R>(&mut self, map_op: &'a F, args: &'a A) -> Option<R>
    where
        A: Args<'a>,
        F: Fn(T, &'a A::Item) -> R,
    {
        match self {
            SplitItem::Some(item, range) => {
                if range.len() == 0 { return None; }
                debug_assert!(range.end <= args.len());

                let arg = args.get(range.start);
                range.start += 1;

                Some(map_op(item.clone(), arg))
            }
            SplitItem::Empty => None,
        }
    }

    fn iter_map<'a, F, A, R>(self, map_op: &'a F, args: &'a A) -> iter::Map<A::Iter, F>
    where
        A: Args<'a>,
        F: Fn(T, &'a A::Item) -> R,
    {
        args.iter_range(self.)
    }
}


// trait Split<T>
//     where Self: Sized,
// {
//     fn len(&self) -> usize;
//
//     fn split_at(self, index: usize) -> (Self, Self);
// }
//
// impl<T: Clone> Split<T> for Option<(T, Range<usize>)>{
//     fn len(&self) -> usize {
//         match self {
//             Some((_, range)) => range.len(),
//             None => 0,
//         }
//     }
//
//     fn split_at(self, index: usize) -> (Self, Self) {
//         match self {
//             Some((item, range)) => {
//                 debug_assert!(0 < index && index <= range.end );
//
//                 let (l_range, r_range) = (range.start..index, index..range.end);
//
//                 // Split into left and right sides, avoiding unnecessary cloning.
//                 if r_range.is_empty() {
//                     (Some((item, l_range)), None)
//                 } else if l_range.is_empty() {
//                     (None, Some((item, r_range)))
//                 } else {
//                     (Some((item.clone(), l_range)), Some((item, r_range)))
//                 }
//             }
//             None => {
//                 debug_assert!(index == 0);
//                 (None, None)
//             }
//         }
//     }
// }
