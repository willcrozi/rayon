// Initial attempt with MapArgs as a struct.

use std::iter::Map;
use crate::iter::Repeat;
use std::iter;

// We need three distinct objects:
//
//  * An initial 'source' of the arguments.
//    Held by: Producer (alongside base iterator and owned F)
//
//  * A splittable version

pub(crate) trait Args: IntoIterator + Sized
    where Self::IntoIter: ExactSizeIterator
{
    fn len(&self) -> usize;
    fn split_at(self, index: usize) -> (Self, Self);
    fn pop(&mut self) -> Self::Item;

    fn map_item<F, T>(self, map_op: &F, item: T) -> MapArgs<'_, F, T, Self> {
        MapArgs { item, map_op, args: self }
    }
}

// TODO Using a struct here might be more restrictive than using a trait. For example it might be
//      possible to perform splits in a more efficient way when implementing a trait for some
//      provded type here
pub(crate) struct MapArgs<'f, F, T, A> {
    item: T,
    map_op: &'f F,
    args: A,
}

impl<'f, F, T, A, R> MapArgs<'f, F, T, A>
    where
        F: Fn(T, A::Item) -> R,
        T: Clone,
        A: Args,
        A::IntoIter: ExactSizeIterator,
{
    pub fn split_at(self, index: usize) -> (Self, Self) {
        let (l_args, r_args) = self.args.split_at(index);
        (
            MapArgs { item: self.item.clone(), map_op: self.map_op, args: l_args },
            MapArgs { item: self.item, map_op: self.map_op, args: r_args },
        )
    }

    pub fn pop(&mut self) -> R {
        (self.map_op)(self.item.clone(), self.args.pop())
    }
}

impl<'f, F, T, A, R> IntoIterator for MapArgs<'f, F, T, A>
    where
        F: Fn(T, A::Item) -> R,
        T: Clone,
        A: Args,
        A::IntoIter: ExactSizeIterator,
{
    type Item = R;
    type IntoIter = MapArgsIter;

    fn into_iter(self) -> Self::IntoIter {

    }
}

struct MapArgsIter<'f, 't, F, T, A,> {
    map_op: &'f F,
    item: &'t T,
    args: A::IntoIter,
}

impl Iterator for MapArgsIter<'f, 't, F, T, A,>
