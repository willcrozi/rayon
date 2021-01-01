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
                  T: Clone + Send,
                  F: for<'a> Fn(T, &'a <A as Args<'a>>::Item) -> R + Sync,
                  A: for<'a> Args<'a>,
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
                  T: Clone + Send,
                  F: for<'a> Fn(T, &'a <A as Args<'a>>::Item) -> R + Sync,
                  A: for<'a> Args<'a>,
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


struct FlatMapExactProducer<'a, P, F, A>
    where P: PopProducer,
          A: Args<'a>,
{
    base: P,
    map_op: &'a F,
    args: &'a A,
    front: Option<SplitItem<P::Item>>,
    back: Option<SplitItem<P::Item>>,
    len: usize,
}

impl<'a, P, F, A, R> FlatMapExactProducer<'a, P, F, A>
    where P: PopProducer,
          P::Item: Clone,
          A: Args<'a>,
          F: Fn(P::Item, &'a A::Item) -> R + Sync,
          R: Send,
{
    fn new(
        base: P,
        map_op: &'a F,
        args: &'a A,
        front: Option<SplitItem<P::Item>>,
        back: Option<SplitItem<P::Item>>,
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
        FlatMapExactIter::new(self.base.into_iter(), self.map_op, self.args, self.front, self.back)
    }

    fn split_at(mut self, index: usize) -> (Self, Self) {
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
                FlatMapExactProducer::new(l_base, self.map_op, self.args, l_front, None, l_len),
                FlatMapExactProducer::new(r_base, self.map_op, self.args, r_front, self.back, r_len),
            )

        } else {
            let back_len = match self.back {
                Some(ref back) => back.len(),
                None => 0,
            };

            // First logical index that is past the base.
            let base_end = self.len - back_len;

            if index <= base_end {
                // Split occurs 'within' the base producer.

                // If there is a 'split' base item (i.e. both left and right sides need to produce
                // an item based on a single base item), then we pop it from the right hand base to
                // allow us to populate it as left's last item and right's first item.

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
                        (
                            Some(SplitItem(item.clone(), 0..arg_index)),
                            Some(SplitItem(item, arg_index..args_len))
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
                let (l_back, r_front) = match self.back {
                    Some(back) => {
                        let (left, right) = back.split_at(index);
                        (Some(left), Some(right))
                    }
                    None => (None, None)
                };

                (
                    FlatMapExactProducer::new(l_base, self.map_op, self.args.clone(), self.front, l_back, l_len),
                    FlatMapExactProducer::new(r_base, self.map_op, self.args, r_front, None, r_len),
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

        // TODO investigate if this is correct here: it would appear that complete() is the place
        //      to consume the rear item, if any, but it appears that the base consumer may assert
        //      the correct length before this gets called.
        // Consume the last item before any correctness assertions are performed.
        let mut folder = self.base.fold_with(folder1);

        if !folder.base.full() {
            if let Some(SplitItem(item, range)) = folder.back {
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
          R: Send,
{
    fn try_pop(&mut self) -> Option<Self::Item> {
        let (map_op, args) = (self.map_op, self.args);

        // Try the front.
        if let Some(ref mut front) = self.front {
            if let Some(mapped) = front.pop_map(map_op, args) {
                return Some(mapped);
            } else {
                // Front is empty, 'fuse' it.
                self.front = None;
            }
        }

        // Front is empty try base producer.
        if let Some(item) = self.base.try_pop() {
            // We have to check for empty args list here...
            if args.len() == 0 { return None; }

            // Since we've popped from the base we now need a 'partial' item at the front.
            self.front = Some(SplitItem(item.clone(), 1..args.len()));

            let mapped = map_op(item, args.get(0));
            return Some(mapped);
        }

        // Base is empty, try the back.
        if let Some(ref mut back) = self.back {
            if let Some(mapped) = back.pop_map(map_op, args) {
                return Some(mapped);
            } else {
                // Back is empty, 'fuse' it.
                self.back = None;
            }
        };

        // Producer empty.
        None
    }
}

struct FlatMapExactIter<'a, I, F, A>
where
    I: Iterator,
    I::Item: Clone,
    A: Args<'a>,
{
    base: iter::Fuse<I>,
    map_op: &'a F,
    args: &'a A,
    front: Option<ArgsIter<'a, I::Item, A>>,
    back: Option<ArgsIter<'a, I::Item, A>>,
}

impl<'a, I, F, A, R> FlatMapExactIter<'a, I, F, A>
    where
        I: Iterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: Args<'a>,
{
    fn new(
        mut base: I,
        map_op: &'a F,
        args: &'a A,
        front: Option<SplitItem<I::Item>>,
        back: Option<SplitItem<I::Item>>) -> Self
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
                    SplitItem(item, 0..args.len())
                        .args_iter(args)
                )
            },
        };

        let back = back.map(|back| back.args_iter(args));

        FlatMapExactIter { base, map_op, args, front, back }
    }
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
            let mut front = SplitItem(item, 0..self.args.len())
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

impl<'a, I, F, A, R> DoubleEndedIterator for FlatMapExactIter<'a, I, F, A>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: Args<'a>,
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
            let mut back = SplitItem(item, 0..self.args.len())
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

impl<'a, I, F, A, R> ExactSizeIterator for FlatMapExactIter<'a, I, F, A>
    where
        I: DoubleEndedIterator + ExactSizeIterator,
        I::Item: Clone,
        F: Fn(I::Item, &'a A::Item) -> R,
        A: Args<'a>,
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
          T: Clone,
          A: Args<'a>,
{
    type Folder = FlatMapExactFolder<'a, C::Folder, F, T, A>;
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
            front: None,
            back: None,
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
    front: Option<SplitItem<T>>,
    // TODO this is possibly redundant
    back: Option<SplitItem<T>>,
}

impl<'a, C, F, T, A, R> Folder<T> for FlatMapExactFolder<'a, C, F, T, A>
    where
        C: Folder<F::Output>,
        F: Fn(T, &'a A::Item) -> R,
        T: Clone,
        A: Args<'a>,
{
    type Result = C::Result;

    fn consume(mut self, item: T) -> Self {
        let (map_op, args) = (self.map_op, self.args);

        // Consume the front if present.
        if let Some(front) = self.front.take() {
            let iter = front.args_iter(args)
                .map(|(item, arg)| map_op(item, arg));

            self.base = self.base.consume_iter(iter);
        }

        // Finish early if base is full.
        if self.base.full() { return self; }

        // Consume `item`.
        let args_len = self.args.len();
        let iter = SplitItem(item, 0..args_len)
            .args_iter(args)
            .map(|(item, arg)| map_op(item, arg));

        self.base = self.base.consume_iter(iter);

        // NOTE: `self.back` is consumed by `FlatMapExactProducer::fold_with`.

        self
    }

    fn consume_iter<I>(mut self, iter: I) -> Self
        where I: IntoIterator<Item=T>,
    {
        let (map_op, args) = (self.map_op, self.args);

        // Consume the front if present.
        if let Some(front) = self.front.take() {
            let iter = front.args_iter(args)
                .map(|(item, arg)| map_op(item, arg));

            self.base = self.base.consume_iter(iter);
        }

        // Finish early if base is full.
        if self.base.full() { return self; }

        // Consume `iter`.
        let args_len = self.args.len();
        let iter = iter.into_iter()
            .flat_map(|item| {
                SplitItem(item, 0..args_len)
                    .args_iter(args)
                    .map(|(item, arg)| map_op(item, arg))
        });

        self.base = self.base.consume_iter(iter);

        // NOTE: `self.back` is consumed by `FlatMapExactProducer::fold_with`.

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
struct SplitItem<T>(T, Range<usize>);

impl<T: Clone> SplitItem<T> {
    fn len(&self) -> usize { self.1.len() }

    fn split_at(self, index: usize) -> (Self, Self) {
        debug_assert!(index <= self.len());

        let SplitItem(item, range) = self;
        let index = index + range.start;

        (
            SplitItem(item.clone(), range.start..index),
            SplitItem(item, index..range.end)
        )
    }

    // TODO maybe remove the map aspect of this method
    fn pop_map<'a, F, A, R>(&mut self, map_op: &'a F, args: &'a A) -> Option<R>
    where
        A: Args<'a>,
        F: Fn(T, &'a A::Item) -> R,
    {
        if args.len() == 0 { return None; }

        let SplitItem(ref item, ref mut range) = self;

        // Args::get below should panic anyway but for debug this is clearer.
        debug_assert!(range.start < args.len());

        let arg = args.get(range.start);
        range.start += 1;

        Some(map_op(item.clone(), arg))
    }

    fn args_iter<'a, A: Args<'a>>(self, args: &'a A) -> ArgsIter<'a, T, A> {
        ArgsIter { item: self.0, args: args.iter_range(self.1) }
    }
}

// TODO maybe this is redundant and we use Zip<Cycle<T>, A::Iter> directly?
// TODO rename this as well
struct ArgsIter<'a, T: Clone, A: Args<'a>> {
    item: T,
    args: A::Iter,
}

impl<'a, T: Clone, A: Args<'a>> Iterator for ArgsIter<'a, T, A> {
    type Item = (T, &'a A::Item);

    fn next(&mut self) -> Option<Self::Item> {
        self.args.next()
            .map(|arg| (self.item.clone(), arg))
    }
}

impl<'a, T: Clone, A: Args<'a>> ExactSizeIterator for ArgsIter<'a, T, A> {
    fn len(&self) -> usize { self.args.len() }
}

impl<'a, T: Clone, A: Args<'a>> DoubleEndedIterator for ArgsIter<'a, T, A> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.args.next_back()
            .map(|arg| (self.item.clone(), arg))
    }
}
