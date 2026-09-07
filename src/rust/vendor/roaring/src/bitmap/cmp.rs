use core::borrow::Borrow;
use core::cmp::Ordering;
use core::iter::Peekable;

use super::container::Container;
use crate::RoaringBitmap;

impl RoaringBitmap {
    /// Returns true if the set has no elements in common with other. This is equivalent to
    /// checking for an empty intersection.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb1 = RoaringBitmap::new();
    /// let mut rb2 = RoaringBitmap::new();
    ///
    /// rb1.insert(1);
    ///
    /// assert_eq!(rb1.is_disjoint(&rb2), true);
    ///
    /// rb2.insert(1);
    ///
    /// assert_eq!(rb1.is_disjoint(&rb2), false);
    ///
    /// ```
    pub fn is_disjoint(&self, other: &Self) -> bool {
        Pairs::new(&self.containers, &other.containers)
            .filter_map(|(c1, c2)| c1.zip(c2))
            .all(|(c1, c2)| c1.is_disjoint(c2))
    }

    /// Returns `true` if this set is a subset of `other`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb1 = RoaringBitmap::new();
    /// let mut rb2 = RoaringBitmap::new();
    ///
    /// rb1.insert(1);
    ///
    /// assert_eq!(rb1.is_subset(&rb2), false);
    ///
    /// rb2.insert(1);
    ///
    /// assert_eq!(rb1.is_subset(&rb2), true);
    ///
    /// rb1.insert(2);
    ///
    /// assert_eq!(rb1.is_subset(&rb2), false);
    /// ```
    pub fn is_subset(&self, other: &Self) -> bool {
        for pair in Pairs::new(&self.containers, &other.containers) {
            match pair {
                (None, _) => (),
                (_, None) => return false,
                (Some(c1), Some(c2)) => {
                    if !c1.is_subset(c2) {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Returns `true` if this set is a superset of `other`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb1 = RoaringBitmap::new();
    /// let mut rb2 = RoaringBitmap::new();
    ///
    /// rb1.insert(1);
    ///
    /// assert_eq!(rb2.is_superset(&rb1), false);
    ///
    /// rb2.insert(1);
    ///
    /// assert_eq!(rb2.is_superset(&rb1), true);
    ///
    /// rb1.insert(2);
    ///
    /// assert_eq!(rb2.is_superset(&rb1), false);
    /// ```
    pub fn is_superset(&self, other: &Self) -> bool {
        other.is_subset(self)
    }
}

/// An helping Iterator over pairs of containers.
///
/// Returns the smallest container according to its key
/// or both if the key is the same. It is useful when you need
/// to iterate over two containers to do operations on them.
pub(crate) struct Pairs<I, J, L, R>
where
    I: Iterator<Item = L>,
    J: Iterator<Item = R>,
    L: Borrow<Container>,
    R: Borrow<Container>,
{
    left: Peekable<I>,
    right: Peekable<J>,
}

impl<I, J, L, R> Pairs<I, J, L, R>
where
    I: Iterator<Item = L>,
    J: Iterator<Item = R>,
    L: Borrow<Container>,
    R: Borrow<Container>,
{
    pub fn new<A, B>(left: A, right: B) -> Pairs<I, J, L, R>
    where
        A: IntoIterator<Item = L, IntoIter = I>,
        B: IntoIterator<Item = R, IntoIter = J>,
    {
        Pairs { left: left.into_iter().peekable(), right: right.into_iter().peekable() }
    }
}

impl<I, J, L, R> Iterator for Pairs<I, J, L, R>
where
    I: Iterator<Item = L>,
    J: Iterator<Item = R>,
    L: Borrow<Container>,
    R: Borrow<Container>,
{
    type Item = (Option<L>, Option<R>);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.left.peek(), self.right.peek()) {
            (None, None) => None,
            (Some(_), None) => Some((self.left.next(), None)),
            (None, Some(_)) => Some((None, self.right.next())),
            (Some(c1), Some(c2)) => match c1.borrow().key.cmp(&c2.borrow().key) {
                Ordering::Equal => Some((self.left.next(), self.right.next())),
                Ordering::Less => Some((self.left.next(), None)),
                Ordering::Greater => Some((None, self.right.next())),
            },
        }
    }
}
