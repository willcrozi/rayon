use super::plumbing::*;
use super::*;
use crate::math::simplify_range;

use std::ops::Range;

/// `Trim` is an iterator that yields items from the given range of positions _without_
/// the side-effects of iterating the trimmed items.
/// This struct is created by the [`trim()`] method on [`IndexedParallelIterator`]
///
/// [`trim()`]: trait.IndexedParallelIterator.html#method.trim
/// [`IndexedParallelIterator`]: trait.IndexedParallelIterator.html
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
#[derive(Debug, Clone)]
pub struct Trim<I> {
    base: I,
    range: Range<usize>
}

impl<I> Trim<I>
    where
        I: IndexedParallelIterator,
{
    /// Creates a new `Skip` iterator.
    pub(super) fn new(base: I, range: impl RangeBounds<usize>) -> Self {
        let range = simplify_range(range, base.len());
        Trim { base, range }
    }
}

impl<I> ParallelIterator for Trim<I>
    where
        I: IndexedParallelIterator,
{
    type Item = I::Item;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
        where
            C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<I> IndexedParallelIterator for Trim<I>
    where
        I: IndexedParallelIterator,
{
    fn len(&self) -> usize { self.range.len() }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn with_producer<CB>(self, callback: CB) -> CB::Output
        where
            CB: ProducerCallback<Self::Item>,
    {
        return self.base.with_producer(Callback {
            callback,
            range: self.range,
        });

        struct Callback<CB> {
            callback: CB,
            range: Range<usize>,
        }

        impl<T, CB> ProducerCallback<T> for Callback<CB>
            where
                CB: ProducerCallback<T>,
        {
            type Output = CB::Output;
            fn callback<P>(self, base: P) -> CB::Output
                where
                    P: Producer<Item = T>,
            {
                let (_, right) = base.split_at(self.range.start);
                let (left, _) = right.split_at(self.range.len());
                self.callback.callback(left)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn check_trim() {
        let result: Vec<_> = (0..10)
            .into_par_iter()
            .trim(3..=5)
            .collect();

        assert_eq!(result, [3, 4, 5]);
    }
}
