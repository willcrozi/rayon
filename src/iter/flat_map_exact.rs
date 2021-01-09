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
pub struct FlatMapExact<I, A, F> {
    base: I,
    map_op: F,
    args: A,
}

pub(crate) fn flat_map_exact<I, A, F, R>(base: I, args: A, map_op: F)  -> FlatMapExact<I, A, F>
where
    I: ParallelIterator,
    F: Fn(I::Item, &A::Item) -> R + Sync + Send,
    A: ArgSource { FlatMapExact { base, map_op, args } }

impl<I, F, A, R> ParallelIterator for FlatMapExact<I, A, F>
where I: ParallelIterator,
      I::Item: Clone + Send,
      A: ArgSource,
      F: Fn(I::Item, &A::Item) -> R + Sync + Send,
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


impl<I,A, F, R> IndexedParallelIterator for FlatMapExact<I, A, F>
    where I: PopParallelIterator,
          I::Item: Clone + Send,
          F: Fn(I::Item, &A::Item) -> R + Sync + Send,
          A: ArgSource,
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

        struct Callback<CB, A, F> {
            callback: CB,
            map_op: F,
            args: A,
            len: usize
        }

        impl<F, T, A, R, CB> PopProducerCallback<T> for Callback<CB, A, F>
            where CB: ProducerCallback<R>,
                  T: Clone + Send,
                  F: Fn(T, &A::Item) -> R + Sync + Send,
                  A: ArgSource,
                  R: Send,
        {
            type Output = CB::Output;

            fn pop_callback<P>(self, base: P) -> CB::Output
                where P: PopProducer<Item=T>,
            {
                let (map_op, args, len) = (&self.map_op, &self.args, self.len);
                let producer = FlatMapExactProducer::new(base, map_op, args, None, None, len);
                self.callback.callback(producer)
            }
        }
    }
}

impl<I, A, F, R> PopParallelIterator for FlatMapExact<I, A, F>
    where I: PopParallelIterator,
          I::Item: Clone + Send,
          F: Fn(I::Item, &A::Item) -> R + Sync + Send,
          A: ArgSource,
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

        struct Callback<CB, A, F> {
            callback: CB,
            map_op: F,
            args: A,
            len: usize,
        }

        impl<F, T, A, R, CB> PopProducerCallback<T> for Callback<CB, A, F>
            where CB: PopProducerCallback<R>,
                  T: Clone + Send,
                  F: Fn(T, &A::Item) -> R + Sync,
                  A: ArgSource,
                  R: Send,
        {
            type Output = CB::Output;

            fn pop_callback<P>(self, base: P) -> CB::Output
                where
                    P: PopProducer<Item=T>,
            {
                let (map_op, args, len) = (&self.map_op, &self.args, self.len);
                let producer = FlatMapExactProducer::new(base, map_op, args, None, None, len);
                self.callback.pop_callback(producer)
            }
        }
    }
}


struct FlatMapExactProducer<'a, P, A, F>
    where P: PopProducer,
{
    base: P,
    map_op: &'a F,
    args: &'a A,
    front: Option<BaseItem<P::Item>>,
    back: Option<BaseItem<P::Item>>,
    len: usize,
}

impl<'a, P, A, F, R> FlatMapExactProducer<'a, P, A, F>
    where P: PopProducer,
          P::Item: Clone,
          A: ArgSource,
          F: Fn(P::Item, &'a A::Item) -> R + Sync,
          R: Send,
{
    fn new(
        base: P,
        map_op: &'a F,
        args: &'a A,
        front: Option<BaseItem<P::Item>>,
        back: Option<BaseItem<P::Item>>,
        len: usize) -> Self
    {
        FlatMapExactProducer { base, map_op, args, front, back, len }
    }
}

impl<'a, P, A, F, R> Producer for FlatMapExactProducer<'a, P, A, F>
    where P: PopProducer,
          P::Item: Clone + Send,
          F: Fn(P::Item, &'a A::Item) -> R + Sync,
          A: ArgSource,
          R: Send,
{
    type Item = F::Output;
    type IntoIter = FlatMapExactIter<'a, P::IntoIter, A, F>;

    fn into_iter(self) -> Self::IntoIter {
        FlatMapExactIter::new(self.base.into_iter(), self.map_op, self.args, self.front, self.back)
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

        let front_len = match self.front {
            Some(ref front) => front.len(),
            None => 0,
        };

        let r_len = self.len - index;
        let l_len = self.len - r_len;
        let (map_op, args) = (self.map_op, self.args);

        // TODO optimise reuse of of this producer (self) to aid return value optimisation.

        if index <= front_len {
            // Split occurs at the front, left base will be empty.
            let (l_base, r_base) = self.base.split_at(0);
            let (l_front, r_front) = match self.front {
                Some(front) => {
                    let (left, right) = front.split_at(index);
                    (Some(left), Some(right))
                }
                None => (None, None)
            };

            (
                FlatMapExactProducer::new(l_base, map_op, args, l_front, None, l_len),
                FlatMapExactProducer::new(r_base, map_op, args, r_front, self.back, r_len),
            )

        } else {
            let back_len = match self.back {
                Some(ref back) => back.len(),
                None => 0,
            };

            let args_len = self.args.len();

            // Logical index of the start of the back section.
            let back_start = self.len - back_len;

            if index < back_start {
                // Split occurs 'within' the base producer.

                // If there is a 'split' base item (i.e. both left and right sides need to produce
                // an item based on a single base item), then we pop it from the right hand base to
                // allow us to populate it as left's last item and right's first item.

                // How many logical items 'into' the base is the index.
                let base_index = index - front_len;

                let base_split = base_index / args_len;
                let (l_base, mut r_base) = self.base.split_at(base_split);

                let item_split = base_index % args_len;
                let (l_back, r_front) = match item_split {
                    0 => (None, None),
                    _ => {
                        // Split is 'within' a base item.
                        let item = r_base.pop();
                        (
                            Some(BaseItem(item.clone(), 0..item_split)),
                            Some(BaseItem(item, item_split..args_len))
                        )
                    }
                };

                (
                    FlatMapExactProducer::new(l_base, map_op, args, self.front, l_back, l_len),
                    FlatMapExactProducer::new(r_base, map_op, args, r_front, self.back, r_len),
                )

            } else {
                // Split occurs within self.back, right base will be empty.

                // Split base at its 'real' length (i.e. left retains full base, right is empty).
                let base_len = back_start - front_len;
                let base_split = base_len / args_len;
                let (l_base, r_base) = self.base.split_at(base_split);

                let (l_back, r_front) = match self.back {
                    Some(back) => {
                        // We know that index >= back_start from outer if-condition.
                        let back_split = index - back_start;
                        let (left, right) = back.split_at(back_split);

                        (Some(left), Some(right))
                    }
                    None => (None, None),
                };

                (
                    FlatMapExactProducer::new(l_base, map_op, args, self.front, l_back, l_len),
                    FlatMapExactProducer::new(r_base, map_op, args, r_front, None, r_len),
                )
            }
        }
    }

    fn fold_with<G>(mut self, folder: G) -> G
        where
            G: Folder<Self::Item>,
    {

        let mut folder1 = FlatMapExactFolder {
            base: folder,
            map_op: self.map_op,
            args: self.args,
        };

        // Consume the front, if present.
        if let Some(front) = self.front.take() {
            let iter = front.args_iter(self.args)
                .map(|(item, arg)| (self.map_op)(item.clone(), arg));

            folder1.base = folder1.base.consume_iter(iter);
        }

        // Consume the base.
        let mut folder = self.base.fold_with(folder1);

        // Consume the back, if present.
        if let Some(back) = self.back.take() {
            let map_op = folder.map_op;
            let iter = back.args_iter(self.args)
                .map(|(item, arg)| map_op(item.clone(), arg));

            folder.base = folder.base.consume_iter(iter);
        }

        folder.base
    }
}

impl<'a, P, A, F, R> PopProducer for FlatMapExactProducer<'a, P, A, F>
    where P: PopProducer,
          P::Item: Clone + Send,
          F: Fn(P::Item, &'a A::Item) -> R + Sync,
          A: ArgSource,
          R: Send,
{
    fn try_pop(&mut self) -> Option<Self::Item> {
        let (map_op, args) = (self.map_op, self.args);

        // Try the front.
        if let Some(ref mut front) = self.front {
            if let Some((item, arg)) = front.pop_args(args) {
                return Some((self.map_op)(item, arg));
            } else {
                // Front is empty, 'fuse' it.
                self.front = None;
            }
        }

        // Front is empty, try base producer.
        if let Some(item) = self.base.try_pop() {
            // We have to check for empty args list here...
            if args.len() == 0 { return None; }

            // Since we've popped from the base we now need a 'partial' item at the front.
            self.front = Some(BaseItem(item.clone(), 1..args.len()));

            let mapped = map_op(item, args.get(0));
            return Some(mapped);
        }

        // Base is empty, try the back.
        if let Some(ref mut back) = self.back {
            if let Some((item, arg)) = back.pop_args(args) {
                return Some((self.map_op)(item, arg));
            } else {
                // Back is empty, 'fuse' it.
                self.back = None;
            }
        };

        // Producer empty.
        None
    }
}

struct FlatMapExactIter<'a, I, A, F>
where
    I: Iterator,
    I::Item: Clone,
    A: ArgSource,
{
    base: iter::Fuse<I>,
    map_op: &'a F,
    args: &'a A,
    front: Option<ArgsIter<'a, I::Item, A>>,
    back: Option<ArgsIter<'a, I::Item, A>>,
}

impl<'a, I, F, A, R> FlatMapExactIter<'a, I, A, F>
    where
        I: Iterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: ArgSource,
{
    fn new(
        base: I,
        map_op: &'a F,
        args: &'a A,
        front: Option<BaseItem<I::Item>>,
        back: Option<BaseItem<I::Item>>) -> Self
    {
        // TODO review if we need Fuse
        let mut base = base.fuse();

        // The front iterator is either the front partial item/args (if present), the first
        // base item/args, or none.
        let front = match front {
            Some(front) => Some(front.args_iter(args)),
            None => {
                // This breaks 'pure' laziness but since the producer has likely popped items
                // already from its base producer, it's not a deal breaker.
                base.next().map(|item|
                    BaseItem(item, 0..args.len())
                        .args_iter(args)
                )
            },
        };

        let back = back.map(|back| back.args_iter(args));

        FlatMapExactIter { base, map_op, args, front, back }
    }
}

impl<'a, I, A, F, R> Iterator for FlatMapExactIter<'a, I, A, F>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: ArgSource,
{
    type Item = F::Output;

    fn next(&mut self) -> Option<Self::Item> {
        // This is a double-ended iterator and we are iterating from the front here. This means we
        // replace the front sub-iterator when exhausted and only consume the back sub-iterator once
        // the base iterator is exhausted.

        // Try the front.
        if let Some(ref mut front) = self.front {
            if let Some((item, arg)) = front.next() {
                return Some((self.map_op)(item, arg));
            }
        }

        // Try to replenish the font and return the mapping of its first item/arg tuple.
        if let Some(item) = self.base.next() {
            let mut front = BaseItem(item, 0..self.args.len())
                .args_iter(self.args);

            let result = front.next().map(|(item, arg)| (self.map_op)(item, arg));
            self.front = Some(front);

            // The only case where `result == None` is when `self.args.len() == 0` which is correct
            // since in this case we would never yield an item.
            return result;
        } else {
            self.front = None;
        }

        // Front and base are empty, try the back.
        if let Some(ref mut back) = self.back {
            if let Some((item, arg)) = back.next() {
                return Some((self.map_op)(item, arg));
            } else {
                self.back = None;
            }
        }

        // Iterator exhausted
        None
    }


    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'a, I, A, F, R> DoubleEndedIterator for FlatMapExactIter<'a, I, A, F>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: ArgSource,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        // This is a double-ended iterator and we are iterating from the back here. This means we
        // replace the back sub-iterator when exhausted and only consume the front sub-iterator once
        // the base iterator is exhausted.

        // Try the back.
        if let Some(ref mut back) = self.back {
            if let Some((item, arg)) = back.next_back() {
                return Some((self.map_op)(item, arg));
            }
        }

        // Try to replenish the back and return the mapping of its first item/arg tuple.
        if let Some(item) = self.base.next_back() {
            let mut back = BaseItem(item, 0..self.args.len())
                .args_iter(self.args);

            let result = back.next_back().map(|(item, arg)| (self.map_op)(item, arg));
            self.back = Some(back);

            // The only case where `result == None` is when `self.args.len() == 0` which is correct
            // since in this case we would never yield an item.
            return result;
        } else {
            self.back = None;
        }

        // Back and base are empty, try the front.
        if let Some(ref mut front) = self.front {
            if let Some((item, arg)) = front.next_back() {
                return Some((self.map_op)(item, arg));
            } else {
                self.front = None;
            }
        }

        // Iterator exhausted
        None
    }
}

impl<'a, I, A, F, R> ExactSizeIterator for FlatMapExactIter<'a, I, A, F>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: ArgSource,
{
    fn len(&self) -> usize {
        let mut len = self.base.len().checked_mul(self.args.len());

        if let Some(ref front) = self.front {
            len = len.and_then(|len| len.checked_add(front.len()));
        }

        if let Some(ref back) = self.back {
            len = len.and_then(|len| len.checked_add(back.len()));
        }

        len.expect("overflow")
    }
}

struct FlatMapExactConsumer<'a, C, A, F> {
    base: C,
    map_op:  &'a F,
    args: &'a A,
}

impl<'f, C, F, A> FlatMapExactConsumer<'f, C, A, F>{
    fn new(base: C, map_op: &'f F, args: &'f A) -> Self { FlatMapExactConsumer { base, map_op, args } }
}

impl<'a, C, F, A, T, R> Consumer<T> for FlatMapExactConsumer<'a, C, A, F>
    where C: Consumer<F::Output>,
          F: Fn(T, &'a A::Item) -> R + Sync,
          T: Clone,
          A: ArgSource,
{
    type Folder = FlatMapExactFolder<'a, C::Folder, A, F>;
    type Reducer = C::Reducer;
    type Result = C::Result;

    // NOTE: With a Consumer base is the destination for our elements, caller is the source
    fn split_at(mut self, index: usize) -> (Self, Self, Self::Reducer) {

        let base_index = index.checked_mul(self.args.len()).expect("overflow");
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
        }
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

impl<'a, T, C, A, F, R> UnindexedConsumer<T> for FlatMapExactConsumer<'a, C, A, F>
    where C: UnindexedConsumer<F::Output>,
          F: Fn(T, &'a A::Item) -> R + Sync,
          T: Clone + Send,
          A: ArgSource,
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

struct FlatMapExactFolder<'f, C, A, F> {
    base: C,
    map_op: &'f F,
    args: &'f A,
}

impl<'a, C, F, T, A, R> Folder<T> for FlatMapExactFolder<'a, C, A, F>
    where
        C: Folder<F::Output>,
        F: Fn(T, &'a A::Item) -> R,
        T: Clone,
        A: ArgSource,
{
    type Result = C::Result;

    fn consume(mut self, item: T) -> Self {
        let (map_op, args) = (self.map_op, self.args);

        // Consume `item`.
        let args_len = self.args.len();
        let iter = BaseItem(item, 0..args_len)
            .args_iter(args)
            .map(|(item, arg)| map_op(item, arg));

        self.base = self.base.consume_iter(iter);
        self
    }

    fn consume_iter<I>(mut self, iter: I) -> Self
        where I: IntoIterator<Item=T>,
    {
        let (map_op, args) = (self.map_op, self.args);

        // Consume `iter`.
        let args_len = self.args.len();
        let iter = iter.into_iter()
            .flat_map(|item| {
                BaseItem(item, 0..args_len)
                    .args_iter(args)
                    .map(|(item, arg)| map_op(item, arg))
        });

        self.base = self.base.consume_iter(iter);

        self
    }

    fn complete(self) -> C::Result {
        self.base.complete()
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

// TODO this is a shit name, please rename!
struct BaseItem<T>(T, Range<usize>);

impl<T: Clone> BaseItem<T> {
    fn len(&self) -> usize { self.1.len() }

    fn split_at(self, index: usize) -> (Self, Self) {
        debug_assert!(index <= self.len());

        let BaseItem(item, range) = self;
        let index = index + range.start;

        (
            BaseItem(item.clone(), range.start..index),
            BaseItem(item, index..range.end)
        )
    }

    fn pop_args<'a, A: ArgSource>(&mut self, args: &'a A) -> Option<(T, &'a A::Item)> {
        if args.len() == 0 { return None; }

        let BaseItem(ref item, ref mut range) = self;

        // Debug
        if range.start >= args.len() {
            println!("BaseItem range: {:?}", range);
        }

        // Args::get below should panic anyway but for debug this is clearer.
        debug_assert!(range.start < args.len());

        let arg = args.get(range.start);
        range.start += 1;

        Some((item.clone(), arg))
    }

    fn args_iter<A: ArgSource>(self, args: &A) -> ArgsIter<'_, T, A> {
        ArgsIter::new(self.0, args, self.1)
    }
}

// TODO maybe this is redundant and we use Zip<Cycle<T>, A::Iter> directly?
// TODO rename this as well
struct ArgsIter<'a, T: Clone, A> {
    item: T,
    args: &'a A,
    range: Range<usize>,
}

impl<'a, T: Clone, A: ArgSource> ArgsIter<'a, T, A> {
    fn new(item: T, args: &'a A, range: Range<usize>) -> Self {
        debug_assert!(range.end <= args.len());
        ArgsIter { item, args, range }
    }
}

impl<'a, T: Clone, A: ArgSource> Iterator for ArgsIter<'a, T, A> {
    type Item = (T, &'a A::Item);

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next()
            .map(|index| (self.item.clone(), self.args.get(index)))
    }
}

impl<'a, T: Clone, A: ArgSource> ExactSizeIterator for ArgsIter<'a, T, A> {
    fn len(&self) -> usize { self.args.len() }
}

impl<'a, T: Clone, A: ArgSource> DoubleEndedIterator for ArgsIter<'a, T, A> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back()
            .map(|index| (self.item.clone(), self.args.get(index)))
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use crate::iter::{ParallelIterator, IntoParallelIterator, IndexedParallelIterator};
    use crate::iter::util::set_rayon_threads;

    #[test]
    fn check_flat_map_exact() {
        let multipliers: &[usize] = &[1, 10, 100];

        // For each input length try all possible 'skip' and 'take' counts.
        for base_len in 0..16 {
            let len = base_len * multipliers.len();
            for skip in 0..=len {
                for take in 0..(len - skip) {
                    let par_nums = (0..len).into_par_iter()
                        .flat_map_exact(multipliers, |n, m| n * m)
                        .skip(skip)
                        .take(take)
                        .collect::<Vec<_>>();

                    let seq_nums = (0..len)
                        .flat_map(move |n| multipliers.iter().map(move |m| n * m))
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

    #[test]
    fn check_combo() {
        set_rayon_threads(1);

        let nums = vec![1, 2, 3];
        let deltas = vec![0, 1, 2];

        let par_nums = nums.clone().into_par_iter()
            .map_interleave(|num| num * 10)
            .flat_map_exact(&deltas[..], |num, delta| num + *delta)
            .skip(1)
            .collect::<Vec<_>>();

        let seq_nums = nums.into_iter()
            .flat_map(|num| vec![num, num * 10])
            .flat_map(|num| deltas.iter().map(move |delta| num + delta))
            .skip(1)
            .collect::<Vec<_>>();

        assert_eq!(par_nums, seq_nums);

    }
}
