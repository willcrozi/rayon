use std::cmp::min;

use super::plumbing::*;
use super::*;
use crate::math::div_round_up;

/// `ChunksFold` is an iterator that groups elements of an underlying iterator and applies a
/// function over them, producing a single value for each group.
///
/// This struct is created by the [`chunks_fold()`] method on [`IndexedParallelIterator`]
///
/// [`chunks_fold()`]: trait.IndexedParallelIterator.html#method.chunks_fold
/// [`IndexedParallelIterator`]: trait.IndexedParallelIterator.html
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
#[derive(Debug, Clone)]
pub struct ChunksFold<I, ID, F>
where
    I: IndexedParallelIterator,
{
    base: I,
    chunk_size: usize,
    fold_op: F,
    identity: ID,
}

impl<I, ID, U, F> ChunksFold<I, ID, F>
where
    I: IndexedParallelIterator,
    ID: Fn() -> U + Send + Sync,
    F: Fn(U, I::Item) -> U + Send + Sync,
{
    /// Creates a new `ChunksFold` iterator
    pub(super) fn new(base: I, chunk_size: usize, identity: ID, fold_op: F, ) -> Self {
        ChunksFold { base, chunk_size, identity, fold_op }
    }
}

impl<I, ID, U, F> ParallelIterator for ChunksFold<I, ID, F>
where
    I: IndexedParallelIterator,
    ID: Fn() -> U + Send + Sync,
    F: Fn(U, I::Item) -> U + Send + Sync,
    U: Send,
{
    type Item = U;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: Consumer<U>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<I, ID, U, F> IndexedParallelIterator for ChunksFold<I, ID, F>
where
    I: IndexedParallelIterator,
    ID: Fn() -> U + Send + Sync,
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
            len,
            identity: self.identity,
            fold_op: self.fold_op,
            callback,
        });

        struct Callback<CB, ID, F> {
            chunk_size: usize,
            len: usize,
            identity: ID,
            fold_op: F,
            callback: CB,
        }

        impl<T, CB, ID, U, F> ProducerCallback<T> for Callback<CB, ID, F>
        where
            CB: ProducerCallback<U>,
            ID: Fn() -> U + Send + Sync,
            F: Fn(U, T) -> U + Send + Sync,
        {
            type Output = CB::Output;

            fn callback<P>(self, base: P) -> CB::Output
            where
                P: Producer<Item = T>,
            {
                self.callback.callback(ChunksFoldProducer {
                    chunk_size: self.chunk_size,
                    len: self.len,
                    identity: &self.identity,
                    fold_op: &self.fold_op,
                    base,
                })
            }
        }
    }
}

struct ChunksFoldProducer<'f, P, ID, F>
where
    P: Producer,
{
    chunk_size: usize,
    len: usize,
    identity: &'f ID,
    fold_op: &'f F,
    base: P,
}

impl<'f, P, ID, U, F> Producer for ChunksFoldProducer<'f, P, ID, F>
where
    P: Producer,
    ID: Fn() -> U + Send + Sync,
    F: Fn(U, P::Item) -> U + Send + Sync,
{
    type Item = F::Output;
    type IntoIter = ChunksFoldSeq<'f, P, ID, F>;

    fn into_iter(self) -> Self::IntoIter {
        ChunksFoldSeq {
            chunk_size: self.chunk_size,
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
            ChunksFoldProducer {
                chunk_size: self.chunk_size,
                len: elem_index,
                identity: self.identity,
                fold_op: self.fold_op,
                base: left,
            },
            ChunksFoldProducer {
                chunk_size: self.chunk_size,
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

struct ChunksFoldSeq<'f, P, ID, F> {
    chunk_size: usize,
    len: usize,
    identity: &'f ID,
    fold_op: &'f F,
    inner: Option<P>,
}

impl<'f, P, ID, U, F> Iterator for ChunksFoldSeq<'f, P, ID, F>
where
    P: Producer,
    ID: Fn() -> U + Send + Sync,
    F: Fn(U, P::Item) -> U + Send + Sync,
{
    type Item = U;

    fn next(&mut self) -> Option<Self::Item> {
        let producer = self.inner.take()?;
        if self.len > self.chunk_size {
            let (left, right) = producer.split_at(self.chunk_size);
            self.inner = Some(right);
            self.len -= self.chunk_size;
            let chunk = left.into_iter();
            Some(chunk.fold((self.identity)(), self.fold_op))
        } else {
            debug_assert!(self.len > 0);
            self.len = 0;
            let chunk = producer.into_iter();
            Some(chunk.fold((self.identity)(), self.fold_op))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'f, P, ID, U, F> ExactSizeIterator for ChunksFoldSeq<'f, P, ID, F>
where
    P: Producer,
    ID: Fn() -> U + Send + Sync,
    F: Fn(U, P::Item) -> U + Send + Sync,
{
    #[inline]
    fn len(&self) -> usize {
        div_round_up(self.len, self.chunk_size)
    }
}

impl<'f, P, ID, U, F> DoubleEndedIterator for ChunksFoldSeq<'f, P, ID, F>
where
    P: Producer,
    ID: Fn() -> U + Send + Sync,
    F: Fn(U, P::Item) -> U + Send + Sync,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let producer = self.inner.take()?;
        if self.len > self.chunk_size {
            let mut size = self.len % self.chunk_size;
            if size == 0 {
                size = self.chunk_size;
            }
            let (left, right) = producer.split_at(self.len - size);
            self.inner = Some(left);
            self.len -= size;
            let chunk = right.into_iter();
            Some(chunk.fold((self.identity)(), self.fold_op))
        } else {
            debug_assert!(self.len > 0);
            self.len = 0;
            let chunk = producer.into_iter();
            Some(chunk.fold((self.identity)(), self.fold_op))
        }
    }
}

#[test]
fn check_chunks_fold() {
    let words = "bishbashbosh!".chars().collect::<Vec<_>>()
        .into_par_iter()
        .chunks_fold(
            4,
            || String::new(),
            |mut s: String, c| { s.push(c); s } )
        .collect::<Vec<_>>();

    assert_eq!(words, vec!["bish", "bash", "bosh", "!"]);
}
