use std::cmp::min;

use super::plumbing::*;
use super::*;
use crate::math::div_round_up;

/// `ChunksMaybeFold` is an iterator that groups elements of an underlying iterator and applies a
/// function over them, producing a single value for each group.
///
/// This struct is created by the [`chunks_maybe_fold()`] method on [`IndexedParallelIterator`]
///
/// [`chunks_maybe_fold()`]: trait.IndexedParallelIterator.html#method.chunks_maybe_fold
/// [`IndexedParallelIterator`]: trait.IndexedParallelIterator.html
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
#[derive(Debug, Clone)]
pub struct ChunksMaybeFold<I, ID, F>
where
    I: IndexedParallelIterator,
{
    base: I,
    chunk_size: usize,
    fold_op: F,
    identity: ID,
}

impl<I, ID, U, F> ChunksMaybeFold<I, ID, F>
where
    I: IndexedParallelIterator,
    ID: Fn(usize) -> Option<U> + Send + Sync,
    F: Fn(U, I::Item) -> U + Send + Sync,
{
    /// Creates a new `ChunksMaybeFold` iterator
    pub(super) fn new(base: I, chunk_size: usize, identity: ID, fold_op: F, ) -> Self {
        ChunksMaybeFold { base, chunk_size, identity, fold_op }
    }
}

impl<I, ID, U, F> ParallelIterator for ChunksMaybeFold<I, ID, F>
where
    I: IndexedParallelIterator,
    ID: Fn(usize) -> Option<U> + Send + Sync,
    F: Fn(U, I::Item) -> U + Send + Sync,
    U: Send,
{
    type Item = Option<U>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: Consumer<Option<U>>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<I, ID, U, F> IndexedParallelIterator for ChunksMaybeFold<I, ID, F>
where
    I: IndexedParallelIterator,
    ID: Fn(usize) -> Option<U> + Send + Sync,
    F: Fn(U, I::Item) -> U + Send + Sync,
    U: Send,
{
    fn drive<C>(self, consumer: C) -> C::Result
    where
        C: Consumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn len(&self) -> usize {
        div_round_up(self.base.len(), self.chunk_size)
    }

    fn with_producer<CB>(self, callback: CB) -> CB::Output
    where
        CB: ProducerCallback<Self::Item>,
    {
        let len = self.base.len();
        return self.base.with_producer(Callback {
            chunk_size: self.chunk_size,
            pos: 0,
            len,
            identity: self.identity,
            fold_op: self.fold_op,
            callback,
        });

        struct Callback<CB, ID, F> {
            chunk_size: usize,
            pos: usize,
            len: usize,
            identity: ID,
            fold_op: F,
            callback: CB,
        }

        impl<T, CB, ID, U, F> ProducerCallback<T> for Callback<CB, ID, F>
        where
            CB: ProducerCallback<Option<U>>,
            ID: Fn(usize) -> Option<U> + Send + Sync,
            F: Fn(U, T) -> U + Send + Sync,
        {
            type Output = CB::Output;

            fn callback<P>(self, base: P) -> CB::Output
            where
                P: Producer<Item = T>,
            {
                self.callback.callback(ChunksMaybeFoldProducer {
                    chunk_size: self.chunk_size,
                    pos: self.pos,
                    len: self.len,
                    identity: &self.identity,
                    fold_op: &self.fold_op,
                    base,
                })
            }
        }
    }
}

struct ChunksMaybeFoldProducer<'f, P, ID, F>
where
    P: Producer,
{
    chunk_size: usize,
    pos: usize,
    len: usize,
    identity: &'f ID,
    fold_op: &'f F,
    base: P,
}

impl<'f, P, ID, U, F> Producer for ChunksMaybeFoldProducer<'f, P, ID, F>
where
    P: Producer,
    ID: Fn(usize) -> Option<U> + Send + Sync,
    F: Fn(U, P::Item) -> U + Send + Sync,
{
    type Item = Option<F::Output>;
    type IntoIter = ChunksMaybeFoldSeq<'f, P, ID, F>;

    fn into_iter(self) -> Self::IntoIter {
        ChunksMaybeFoldSeq {
            chunk_size: self.chunk_size,
            pos: self.pos,
            len: self.len,
            identity: self.identity,
            fold_op: self.fold_op,
            inner: if self.len > 0 { Some(self.base) } else { None },
        }
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let elem_index = min(index * self.chunk_size, self.len);
        let (left, right) = self.base.split_at(elem_index);
        (
            ChunksMaybeFoldProducer {
                chunk_size: self.chunk_size,
                pos: self.pos,
                len: elem_index,
                identity: self.identity,
                fold_op: self.fold_op,
                base: left,
            },
            ChunksMaybeFoldProducer {
                chunk_size: self.chunk_size,
                pos: self.pos + elem_index,
                len: self.len - elem_index,
                identity: self.identity,
                fold_op: self.fold_op,
                base: right,
            },
        )
    }

    fn min_len(&self) -> usize {
        div_round_up(self.base.min_len(), self.chunk_size)
    }

    fn max_len(&self) -> usize {
        self.base.max_len() / self.chunk_size
    }
}

struct ChunksMaybeFoldSeq<'f, P, ID, F> {
    chunk_size: usize,
    pos: usize,
    len: usize,
    identity: &'f ID,
    fold_op: &'f F,
    inner: Option<P>,
}

impl<'f, P, ID, U, F> Iterator for ChunksMaybeFoldSeq<'f, P, ID, F>
where
    P: Producer,
    ID: Fn(usize) -> Option<U> + Send + Sync,
    F: Fn(U, P::Item) -> U + Send + Sync,
{
    type Item = Option<U>;

    fn next(&mut self) -> Option<Self::Item> {
        let producer = self.inner.take()?;

        let (pos, chunk) = if self.len > self.chunk_size {
            let (left, right) = producer.split_at(self.chunk_size);
            self.inner = Some(right);
            let left_pos = self.pos;
            self.pos += self.chunk_size;
            self.len -= self.chunk_size;
            (left_pos, left)
        } else {
            debug_assert!(self.len > 0);
            let pos = self.pos;
            self.pos += self.len;
            self.len = 0;
            (pos, producer)
        };

        let result = (self.identity)(pos).map(|identity|
            chunk.into_iter()
                .fold(identity, self.fold_op)
        );

        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'f, P, ID, U, F> ExactSizeIterator for ChunksMaybeFoldSeq<'f, P, ID, F>
where
    P: Producer,
    ID: Fn(usize) -> Option<U> + Send + Sync,
    F: Fn(U, P::Item) -> U + Send + Sync,
{
    #[inline]
    fn len(&self) -> usize {
        div_round_up(self.len, self.chunk_size)
    }
}

impl<'f, P, ID, U, F> DoubleEndedIterator for ChunksMaybeFoldSeq<'f, P, ID, F>
where
    P: Producer,
    ID: Fn(usize) -> Option<U> + Send + Sync,
    F: Fn(U, P::Item) -> U + Send + Sync,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let producer = self.inner.take()?;

        let (pos, chunk) = if self.len > self.chunk_size {
            let mut size = self.len % self.chunk_size;
            if size == 0 {
                size = self.chunk_size;
            }
            let (left, right) = producer.split_at(self.len - size);
            self.inner = Some(left);
            // self.pos remains as we are keeping left side
            self.len -= size;
            (self.pos + self.len, right)
        } else {
            debug_assert!(self.len > 0);
            let pos = self.pos;
            self.pos += self.len;
            self.len = 0;
            (pos, producer)
        };

        let result = (self.identity)(pos).map(|identity|
            chunk.into_iter()
                .fold(identity, self.fold_op)
        );

        Some(result)
    }
}

#[test]
fn check_chunks_maybe_fold() {

    let sums = (0..10).into_par_iter()
        .chunks_maybe_fold(
            2,
            |pos| if (2..6).contains(&pos)  { Some(0) } else { None },
            |acc, n| { acc + n } )
        .collect::<Vec<_>>();

    assert_eq!(sums, vec![None, Some(5), Some(9), None, None]);
}
