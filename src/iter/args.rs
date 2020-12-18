// Initial attempt with MapArgs as a struct.

use std::ops::Range;
use std::slice;
use alloc::vec;

// We need three distinct objects:
//
//  * An initial 'source' of the arguments.
//    Held by: Producer (alongside base iterator and owned F)
//
//  * A splittable version

pub(crate) trait Args: Sized + Clone
{
    type Item;
    type SubRange: ExactSizeIterator<Item=Self::Item>;

    fn len(&self) -> usize;
    fn split_at(self, index: usize) -> (Self, Self);
    fn pop(&mut self) -> Self::Item;
    fn sub_range(&self, range: Range<usize>) -> Self::SubRange;
    // fn map_iter<'f, F, T>(self, map_op: F) -> MapArgsIter<'f, F, T, Self>;
}

#[derive(Clone)]
enum PartialArgs<'a, A: Args> {
    Ref(&'a A, Range<usize>),
    Owned(A),
}

impl<'a, A> Args for PartialArgs<'a, A>
    where
        A: Clone,
        A: Args + 'a,
{
    type Item = A::Item;
    type SubRange = A::SubRange;

    fn len(&self) -> usize {
        match self {
            PartialArgs::Ref(_, range) => { range.len() }
            PartialArgs::Owned(args) => { args.len() }
        }
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        match self {
            PartialArgs::Ref(args, range) => {
                debug_assert!(index <= range.len());
                let mid = range.start + index;
                (
                    PartialArgs::Ref(args, range.start..mid),
                    PartialArgs::Ref(args, mid..range.end)
                )
            }
            PartialArgs::Owned(args) => {
                let (left, right) = args.split_at(index);
                (PartialArgs::Owned(left), PartialArgs::Owned(right))
            }
        }
    }

    fn pop(&mut self) -> Self::Item {
        match self {
            PartialArgs::Ref(args, range) => {
                let mut args = args.sub_range(range.clone());
                let result = args.pop();
                *self = PartialArgs::Owned(args);
                result
            }
            PartialArgs::Owned(args) => { args.pop() }
        }
    }

    fn sub_range(&self, sub_range: Range<usize>) -> Self::SubRange {
        match self {
            PartialArgs::Ref(args, range) => {
                let start = range.start + sub_range.start;
                let end = range.start + sub_range.end;

                PartialArgs::Ref(args, start..end)
            }
            PartialArgs::Owned(args) => {
                PartialArgs::Owned(args.sub_range(sub_range))
            }
        }
    }
}

// impl<'a, A: Args> IntoIterator for PartialArgs<'a, A> {
//     type Item = A::Item;
//     type IntoIter = A::IntoIter;
//
//     fn into_iter(self) -> Self::IntoIter {
//         match self {
//             PartialArgs::Ref(args, range) => { args.sub_range(range).into_iter() }
//             PartialArgs::Owned(args) => { args.into_iter() }
//         }
//     }
// }

////////////////////////////////////////////////////////////////////////////////

impl<'t, T> Args for &'t[T] {
    type Item = &'t T;
    type SubRange = slice::Iter<'t, T>;

    fn len(&self) -> usize { <[T]>::len(&self) }

    fn split_at(self, index: usize) -> (Self, Self) {
        (&self[0..index], &self[index..0])
    }

    fn pop(&mut self) -> Self::Item {
        let result = &self[0];
        *self = &self[1..];
        result
    }

    fn sub_range(&self, range: Range<usize>) -> Self::SubRange {
        self[&range].iter()
    }
}

impl<'t, T> Args for Vec<T>
where
    T: Clone,
{
    type Item = T;
    type SubRange = vec::IntoIter<T>;

    fn len(&self) -> usize { <[T]>::len(&self) }

    fn split_at(mut self, index: usize) -> (Self, Self) {
        let right = self.split_off(index);
        (self, right)
    }

    fn pop(&mut self) -> Self::Item {
        self.pop().unwrap()
    }

    fn sub_range(&self, range: Range<usize>) -> Self::SubRange {
        self[&range]
    }
}

// impl<I> Args for I
// where
//     I: ExactSizeIterator,
// {
//     fn len(&self) -> usize {
//         self.len()
//     }
//
//     fn split_at(self, index: usize) -> (Self, Self) {
//         unimplemented!()
//     }
//
//     fn pop(&mut self) -> Self::Item {
//         self.next().unwrap()
//     }
//
//     fn sub_range(&self, range: Range<usize>) -> Self {
//         unimplemented!()
//     }
// }



















////////////////////////////////////////////////////////////////////////////////

// struct MapArgsIter<'f, F, T, A: Args> {
//     map_op: &'f F,
//     item: T,
//     args: A::IntoIter,
// }
//
// impl<'f, F, T, A, R> Iterator for MapArgsIter<'f, F, T, A>
// where
//     F: Fn(T, A::Item) -> R,
//     A: Args,
//     T: Clone,
// {
//     type Item = R;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         unimplemented!()
//     }
// }


// TODO Using a struct here might be more restrictive than using a trait. For example it might be
//      possible to perform splits in a more efficient way when implementing a trait for some
//      provded type here
// pub(crate) struct MapArgs<'f, F, T, A> {
//     item: T,
//     map_op: &'f F,
//     args: A,
// }
//
// impl<'f, F, T, A, R> MapArgs<'f, F, T, A>
//     where
//         F: Fn(T, A::Item) -> R,
//         T: Clone,
//         A: Args,
//         A::IntoIter: ExactSizeIterator,
// {
//     pub fn split_at(self, index: usize) -> (Self, Self) {
//         let (l_args, r_args) = self.args.split_at(index);
//         (
//             MapArgs { item: self.item.clone(), map_op: self.map_op, args: l_args },
//             MapArgs { item: self.item, map_op: self.map_op, args: r_args },
//         )
//     }
//
//     pub fn pop(&mut self) -> R {
//         (self.map_op)(self.item.clone(), self.args.pop())
//     }
// }
//
// impl<'f, F, T, A, R> IntoIterator for MapArgs<'f, F, T, A>
//     where
//         F: Fn(T, A::Item) -> R,
//         T: Clone,
//         A: Args,
//         A::IntoIter: ExactSizeIterator,
// {
//     type Item = R;
//     type IntoIter = MapArgsIter;
//
//     fn into_iter(self) -> Self::IntoIter {
//
//     }
// }
//

