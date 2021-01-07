use super::plumbing::*;
use super::*;

use std::iter;
use std::iter::FusedIterator;

// Rayon 'pop' extension notes:
// * API considerations: peek vs pop?
//      Peek style is either ref or clone-and-move based
//          + Ref based allows us to avoid requirement on Clone for Item and to only clone if needed.
//          - Ref based requires some types that implement Peek... to store the value.
//          Move based forces us to require Clone and perform clones

//      Two main options are:
//          Pop style internally with ref returned from peek(), value from base is cached in every
//          type that implements it. Invasive
//          Peek style internally

//      We should leave it up to the implementation of each producer whether or not it should pop
//      or peek internally?
//
//      An argument for pop?:  In the case of an adapter like fork() how would the right side skip
//      the first item.

//      New insights!:
//          * Iam very tired
//          * A parallel iterator's producer (and consumer?) has two distinct interfaces when being
//            'peeked' or 'popped':
//              * its outward facing behaviour/semantics, i.e. from the outside does it behave like
//                a peek or a pop?

// * Figure out how much of all this is relevant to consumers as well as producers.
//
// * Implement for essential base types: vecs, slices, ranges
//
// * Identify and implement for other essential types: Empty? Once? Option? Result?
//
// * Implement for simple 'pass-through' types: Map etc.
//
// * Implement for do-able types like Interleaved, Chained

/// `MapInterleaved` is an iterator that interleaves an iterators items with the result of applying a map
/// operation to each item.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
#[derive(Clone, Debug)]
pub struct MapInterleave<I, F> {
    base: I,
    map_op: F,
}

pub(crate) fn map_interleave<I, F>(base: I, map_op: F) -> MapInterleave<I, F> {
    MapInterleave { base, map_op }
}

impl<I, F> ParallelIterator for MapInterleave<I, F>
    where
        I: ParallelIterator,
        F: Fn(I::Item) -> I::Item + Sync + Send,
        I::Item: Clone + Send,
{
    type Item = I::Item;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
        where
            C: UnindexedConsumer<Self::Item>,
    {
        let consumer1 = MapInterleavedConsumer::new(consumer, &self.map_op);
        self.base.drive_unindexed(consumer1)
    }

    fn opt_len(&self) -> Option<usize> {
        match self.base.opt_len()? {
            0 => Some(0),
            len => len.checked_mul(2),
        }
    }
}

impl<I, F> IndexedParallelIterator for MapInterleave<I, F>
    where
        I: PopParallelIterator,
        I::Item: Clone + Send,
        F: Fn(I::Item) -> I::Item + Sync + Send,
{
    fn drive<C>(self, consumer: C) -> C::Result
        where C: Consumer<Self::Item>,
    {
        let consumer1 = MapInterleavedConsumer::new(consumer, &self.map_op);
        self.base.drive(consumer1)
    }

    fn len(&self) -> usize {
        let len = self.base.len();
        if len > 0 {
            len.checked_mul(2).expect("overflow")
        } else {
            0
        }
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
        // Our callback from base needs a poppable producer in case we split 'within' a base item.
        let len = self.len();
        return self.base.with_pop_producer(Callback {
            callback,
            map_op: self.map_op,
            len,
        });

        struct Callback<CB, F> {
            callback: CB,
            map_op: F,
            len: usize,
        }

        impl<T, F, CB> PopProducerCallback<T> for Callback<CB, F>
            where
                CB: ProducerCallback<T>,
                F: Fn(T) -> T + Sync,
                T: Clone + Send,
        {
            type Output = CB::Output;

            fn pop_callback<P>(self, base: P) -> CB::Output
                where P: PopProducer<Item=T>,
            {
                let producer = MapInterleavedProducer::new(base, &self.map_op, None, None, self.len);
                self.callback.callback(producer)
            }
        }
    }
}

impl<I, F> PopParallelIterator for MapInterleave<I, F>
    where I: PopParallelIterator,
          I::Item: Clone + Send,
          F: Fn(I::Item) -> I::Item + Sync + Send,
{
    fn with_pop_producer<CB>(self, callback: CB) -> CB::Output
        where CB: PopProducerCallback<Self::Item>,
    {
        let len = self.len();
        return self.base.with_pop_producer(Callback {
            callback,
            map_op: self.map_op,
            len,
        });

        struct Callback<CB, F> {
            callback: CB,
            map_op: F,
            len: usize,
        }

        impl<T, F, CB> PopProducerCallback<T> for Callback<CB, F>
            where CB: PopProducerCallback<T>,
                  F: Fn(T) -> T + Sync,
                  T: Clone + Send,
        {
            type Output = CB::Output;

            fn pop_callback<P>(self, base: P) -> CB::Output
                where
                    P: PopProducer<Item=T>,
            {
                let producer = MapInterleavedProducer::new(base, &self.map_op, None, None, self.len);
                self.callback.pop_callback(producer)
            }
        }
    }
}

struct MapInterleavedProducer<'f, P, F>
    where
        P: PopProducer,
{
    base: P,
    map_op: &'f F,
    front: Option<P::Item>,
    back: Option<P::Item>,
    // We need the to track the length as there's no way to get the base length.
    len: usize,
}

impl<'f, P, F> MapInterleavedProducer<'f, P, F>
    where
        P: PopProducer,
        F: Fn(P::Item) -> P::Item + Sync,
{
    fn new(base: P, map_op: &'f F, front: Option<P::Item>, back: Option<P::Item>, len: usize) -> Self {
        MapInterleavedProducer { base, map_op, front, back, len }
    }
}

impl<'f, P, F> Producer for MapInterleavedProducer<'f, P, F>
    where
        P: PopProducer,
        P::Item: Clone + Send,
        F: Fn(P::Item) -> P::Item + Sync,
{
    type Item = F::Output;
    type IntoIter = MapInterleavedIter<'f, P::IntoIter, F>;

    fn into_iter(self) -> Self::IntoIter {
        MapInterleavedIter {
            base: self.base.into_iter().fuse(),
            map_op: self.map_op,
            front: self.front,
            back: self.back,
        }
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        // Working example:
        //
        // Initial logical view of un-split iterator:
        // out:     [1, f(1), 2, f(2), 3, f(3), 4, f(4), 5, f(5)]
        // state:   [1, 2, 3, 4, 5]

        // Subsequent splits:
        // logical:     [1, f(1), 2, f(2), 3]   [f(3), 4, f(4), 5, f(5)]
        // state:       [1, 2] (3)              (f(3)) [4, 5]

        // logical:     [1, f(1), 2]    [f(2), 3]       [f(3), 4, f(4)]     [5, f(5)]
        // state:       [1] (2)         (f(2)) [] (3)   (f(3)) [4]          [5]

        // logical:     [1, f(1)]   [2]     [f(2)]  [3]     [f(3), 4]       [f(4)]  [5]     [f(5)]
        // state:       [1]         [] (2)  (f(2))  [] (3)  (f(3)) [] (4)   (4) []  [] (5)  (5) []

        // * It is only necessary to 'pop' from the split base's right half.
        // * This item is then cloned and placed at rear of left and front of right.
        // * The front item is always consumed as its mapped value, the back item is consumed as is.

        debug_assert!(index <= self.len);

        let front_len = self.front.is_some() as usize;
        let back_len = self.back.is_some() as usize;

        let r_len = self.len - index;
        let l_len = self.len - r_len;

        // TODO optimise reuse of of this producer (self) to aid return value optimisation.

        if index <= 0 {
            // Split occurs at the very front, left will be empty.
            let (l_base, r_base) = self.base.split_at(0);

            (
                MapInterleavedProducer::new(l_base, self.map_op, None, None, l_len),
                MapInterleavedProducer::new(r_base, self.map_op, self.front, self.back, r_len),
            )

        } else {
            // First logical index that is past the base.
            let base_end = self.len - back_len;

            if index <= base_end {
                // Split occurs 'within' the base producer.

                // How many logical items 'into' the base is the index.
                let index_base_offset = index - front_len;

                let split_index = index_base_offset / 2;
                let (l_base, mut r_base) = self.base.split_at(split_index);

                // If there is a 'split' base item (i.e. both left and right hand bases need to
                // produce an item based on a single base item), then we pop it from the right hand
                // base to allow us to populate it as left's last item and right's first item.

                // If there is an item to be split it will be the first item in the split base's
                // right hand side.

                // Check for split across an item.
                let split = match index_base_offset % 2 {
                    1 => Some(r_base.pop()),
                    0 | _ => None,
                };

                (
                    MapInterleavedProducer::new(l_base, self.map_op, self.front, split.clone(), l_len),
                    MapInterleavedProducer::new(r_base, self.map_op, split, self.back, r_len),
                )

            } else {
                debug_assert!(index == self.len);

                // Split occurs at the very back, right will be empty.
                let base_split = base_end / 2; // Ok because front item gets rounded out.
                let (l_base, r_base) = self.base.split_at(base_split);

                (
                    MapInterleavedProducer::new(l_base, self.map_op, self.front, self.back, l_len),
                    MapInterleavedProducer::new(r_base, self.map_op, None, None, r_len),
                )

            }
        }
    }

    fn fold_with<G>(self, folder: G) -> G
        where
            G: Folder<Self::Item>,
    {
        let folder1 = MapInterleavedFolder {
            base: folder,
            map_op: self.map_op,
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
            if let Some(item) = folder.back.take() {
                folder.base = folder.base.consume(item);
            }
        }
        folder.base
    }
}

impl<'f, P, F> PopProducer for MapInterleavedProducer<'f, P, F>
    where P: PopProducer,
          P::Item: Clone + Send,
          F: Fn(P::Item) -> P::Item + Sync,
{
    fn try_pop(&mut self) -> Option<Self::Item> {
        self.front.take()
            .map(self.map_op)
            .or_else(||
                self.base.try_pop().map(|item| {
                    self.front = Some(item.clone());
                    item
                }))
            .or_else(|| self.back.take())
            .map(|item| {
                debug_assert!(self.len > 0);
                self.len -= 1;
                item
            })
    }
}

struct MapInterleavedIter<'f, I, F>
    where I: Iterator,
{
    base: iter::Fuse<I>,
    map_op: &'f F,
    front: Option<I::Item>,
    back: Option<I::Item>,
}

impl<'f, I, F,> Iterator for MapInterleavedIter<'f, I, F>
    where
        I: Iterator,
        I::Item: Clone,
        F: Fn(I::Item) -> I::Item,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // If there is a front item then return its mapped value.
        if let Some(item) = self.front.take() {
            return Some((self.map_op)(item));
        }

        // Otherwise, if there is an item from the base, store a clone at the front and return it.
        if let Some(item) = self.base.next() {
            self.front = Some(item.clone());
            return Some(item);
        }

        // Otherwise, return any item that may be at the back.
        self.back.take()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let base = self.base.size_hint();
        let extra = (self.front.is_some() as usize) +  (self.back.is_some() as usize);
        (base.0 + extra, base.1.map(|len| len + extra))
    }
}

impl<I, F> FusedIterator for MapInterleavedIter<'_, I, F>
    where
        I: Iterator,
        I::Item: Clone,
        F: Fn(I::Item) -> I::Item, {}

impl<'f, I, F> DoubleEndedIterator for MapInterleavedIter<'f, I, F>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone + Send,
        F: Fn(I::Item) -> I::Item + Sync,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        // If there is a back item then return it.
        if self.back.is_some() { return self.back.take(); }

        // Otherwise, if there is an item from the base, store a clone at the back and return its
        // mapped value.
        if let Some(item) = self.base.next_back() {
            self.back = Some(item.clone());
            return Some((self.map_op)(item));
        }

        // Otherwise, return the mapped value of any item that may be at the front.
        self.back.take().map(self.map_op)
    }
}

impl<'f, I, F,> ExactSizeIterator for MapInterleavedIter<'f, I, F,>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone + Send,
        F: Fn(I::Item) -> I::Item + Sync,
{
    fn len(&self) -> usize {
        let base_len = self.base.len();
        let front_len = self.front.is_some() as usize;
        let back_len = self.back.is_some() as usize;

        base_len.checked_add(
            base_len.checked_add(front_len + back_len).expect("overflow")
        ).expect("overflow")
    }
}

struct MapInterleavedConsumer<'f, C, F> {
    base: C,
    map_op:  &'f F,
}

impl<'f, C, F> MapInterleavedConsumer<'f, C, F>{
    fn new(base: C, map_op: &'f F) -> Self { MapInterleavedConsumer { base, map_op } }
}

impl<'f, T, C, F> Consumer<T> for MapInterleavedConsumer<'f, C, F>
    where
        C: Consumer<F::Output>,
        F: Fn(T) -> T + Sync,
        T: Clone + Send,
{
    type Folder = MapInterleavedFolder<'f, C::Folder, F, T>;
    type Reducer = C::Reducer;
    type Result = C::Result;

    fn split_at(mut self, index: usize) -> (Self, Self, Self::Reducer) {
        // We'll always feed twice as many items to the base consumer.
        let base_index = index.checked_mul(2).expect("overflow");
        let (left_base, right_base, reducer) = self.base.split_at(base_index);

        let right = MapInterleavedConsumer {
            base: right_base,
            map_op: self.map_op,
        };

        self.base = left_base;
        (self, right, reducer)
    }

    fn into_folder(self) -> Self::Folder {
        MapInterleavedFolder {
            base: self.base.into_folder(),
            map_op: self.map_op,
            front: None,
            back: None,
        }
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

impl<'f, T, C, F> UnindexedConsumer<T> for MapInterleavedConsumer<'f, C, F>
    where
        C: UnindexedConsumer<T>,
        F: Fn(T) -> T + Sync,
        T: Clone + Send,
{
    fn split_off_left(&self) -> Self {
        let left = MapInterleavedConsumer {
            base: self.base.split_off_left(),
            map_op: self.map_op,
        };
        left
    }

    fn to_reducer(&self) -> Self::Reducer {
        self.base.to_reducer()
    }
}

struct MapInterleavedFolder<'f, C, F, T> {
    base: C,
    map_op: &'f F,
    front: Option<T>,
    // TODO this is possibly redundant
    back: Option<T>,
}

impl<'f, C, F, T> Folder<T> for MapInterleavedFolder<'f, C, F, T>
    where
        C: Folder<F::Output>,
        F: Fn(T) -> T,
        T: Clone,
{
    type Result = C::Result;

    fn consume(mut self, item: T) -> Self {
        // Feeds up to 3 items to base folder: map_op(front), item, map_op(item)

        if let Some(item) = self.front.take() {
            self.base = self.base.consume((self.map_op)(item));
            if self.base.full() { return self; }
        }

        self.base = self.base.consume(item.clone());

        if self.base.full() { return self; }
        self.base = self.base.consume((self.map_op)(item));

        self
    }

    fn consume_iter<I>(mut self, iter: I) -> Self
        where
            I: IntoIterator<Item = T>,
    {
        if let Some(item) = self.front.take() {
            let mapped = (self.map_op)(item);
            self.base = self.base.consume(mapped);
            if self.base.full() { return self; }
        }

        for item in iter.into_iter() {
            self.base = self.base.consume(item.clone());
            if self.base.full() { return self; }

            let mapped = (self.map_op)(item);
            self.base = self.base.consume(mapped);
            if self.base.full() { return self; }
        }

        // TODO investigate if this is correct here: it would appear that complete() is the place
        //      to consume the rear item, if any, but it appears that the base consumer may assert
        //      the correct length before this gets called.
        //      For now the operation is done at <MapInterleaved as Producer>::with_folder()
        // if let Some(item) = self.back.take() {
        //     self.base = self.base.consume(item);
        // }

        self
    }

    fn complete(self) -> C::Result {
        self.base.complete()
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}


////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use crate::iter::{
        ParallelIterator,
        IntoParallelIterator,
        IndexedParallelIterator,
    };

    #[test]
    fn check_map_interleave() {
        // For each input length try all possible 'skip' and 'take' counts.
        for base_len in 0..16 {
            let len = base_len * 2;
            for skip in 0..=len {
                for take in 0..(len - skip) {
                    let par_nums = (0..len).into_par_iter()
                        .map_interleave(|n| n * 10)
                        .skip(skip)
                        .take(take)
                        .collect::<Vec<_>>();

                    let seq_nums = (0..len)
                        .flat_map(|n| vec![n, n * 10])
                        .skip(skip)
                        .take(take)
                        .collect::<Vec<_>>();

                    // eprintln!("len: {}, skip: {}, take: {}", len, skip, take);
                    // eprintln!("par_nums: {:?}", par_nums);
                    assert_eq!(par_nums, seq_nums);
                }
            }
        }
    }
}