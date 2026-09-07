#[cfg(test)]
#[allow(clippy::eq_op)] // Allow equal expressions as operands
mod test {
    use crate::RoaringBitmap;
    use proptest::prelude::*;

    //
    // Tests algebraic set properties in terms of RoaringBitmaps.
    // Follows wikipedia article regarding ordering and heading
    //
    // https://en.wikipedia.org/wiki/Algebra_of_sets
    //
    // Notes:
    //
    //  * Although a universe set exists, we leave properties involving it it out of these tests.
    //    It would be ~512 MiB and operations on it would be relatively slow
    //
    //  * Likewise, there is no compliment operator
    //
    //
    //
    //
    // The fundamental properties of set algebra
    // =========================================
    //
    // Commutative property:
    // --------------------

    proptest! {
        #[test]
        fn unions_are_commutative(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(&a | &b, &b | &a);

            { // op_assign_ref
                let mut x = a.clone();
                let mut y = b.clone();

                x |= &b;
                y |= &a;

                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = a.clone();
                let mut y = b.clone();

                x |= b;
                y |= a;

                prop_assert_eq!(x, y);
            }
        }

        #[test]
        fn intersections_are_commutative(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(&a & &b, &b & &a);

            { // op_assign_ref
                let mut x = a.clone();
                let mut y = b.clone();

                x &= &b;
                y &= &a;

                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = a.clone();
                let mut y = b.clone();

                x &= b;
                y &= a;

                prop_assert_eq!(x, y);
            }
        }

        #[test]
        fn symmetric_differences_are_commutative(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(&a ^ &b, &b ^ &a);

            { // op_assign_ref
                let mut x = a.clone();
                let mut y = b.clone();

                x ^= &b;
                y ^= &a;

                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = a.clone();
                let mut y = b.clone();

                x ^= b;
                y ^= a;

                prop_assert_eq!(x, y);
            }
        }
    }

    //
    // Associative property:
    // ---------------------

    proptest! {
        #[test]
        fn unions_are_associative(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(
                &a | ( &b | &c ),
                ( &a | &b ) | &c
            );

            { // op_assign_ref
                let mut x = b.clone();
                x |= &c;
                x |= &a;

                let mut y = a.clone();
                y |= &b;
                y |= &c;


                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = b.clone();
                x |= c.clone();
                x |= a.clone();

                let mut y = a;
                y |= b;
                y |= c;


                prop_assert_eq!(x, y);
            }
        }

        #[test]
        fn intersections_are_associative(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(
                &a & ( &b & &c ),
                ( &a & &b ) & &c
            );

            { // op_assign_ref
                let mut x = b.clone();
                x &= &c;
                x &= &a;

                let mut y = a.clone();
                y &= &b;
                y &= &c;


                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = b.clone();
                x &= c.clone();
                x &= a.clone();

                let mut y = a;
                y &= b;
                y &= c;


                prop_assert_eq!(x, y);
            }
        }

        #[test]
        fn symmetric_differences_are_associative(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(
                &a ^ ( &b ^ &c ),
                ( &a ^ &b ) ^ &c
            );

            { // op_assign_ref
                let mut x = b.clone();
                x ^= &c;
                x ^= &a;

                let mut y = a.clone();
                y ^= &b;
                y ^= &c;


                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = b.clone();
                x ^= c.clone();
                x ^= a.clone();

                let mut y = a;
                y ^= b;
                y ^= c;


                prop_assert_eq!(x, y);
            }
        }
    }

    //
    // Distributive property:
    // ---------------------

    proptest! {
        #[test]
        fn union_distributes_over_intersection(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(
                &a | ( &b & &c),
                ( &a | &b ) & ( &a | &c )
            );

            { // op_assign_ref
                let mut x = b.clone();
                x &= &c;
                x |= &a;

                let y = {
                    let mut ab = a.clone();
                    ab |= &b;

                    let mut ac = a.clone();
                    ac |= &c;

                    ab &= &ac;
                    ab
                };

                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = b.clone();
                x &= c.clone();
                x |= a.clone();

                let y = {
                    let mut ab = a.clone();
                    ab |= b;

                    let mut ac = a;
                    ac |= c;

                    // moves the owned ac on the rhs
                    ab &= ac;
                    ab
                };

                prop_assert_eq!(x, y);
            }
        }

        #[test]
        fn intersection_distributes_over_union(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(
                &a & ( &b | &c),
                ( &a & &b ) | ( &a & &c )
            );

            { // op_assign_ref
                let mut x = b.clone();
                x |= &c;
                x &= &a;

                let y = {
                    let mut ab = a.clone();
                    ab &= &b;

                    let mut ac = a.clone();
                    ac &= &c;

                    ab |= &ac;
                    ab
                };

                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = b.clone();
                x |= c.clone();
                x &= a.clone();

                let y = {
                    let mut ab = a.clone();
                    ab &= b;

                    let mut ac = a;
                    ac &= c;

                    // moves the owned ac on the rhs
                    ab |= ac;
                    ab
                };

                prop_assert_eq!(x, y);
            }
        }

        #[test]
        fn intersection_distributes_over_symmetric_difference(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(
                &a & ( &b ^ &c),
                ( &a & &b ) ^ ( &a & &c )
            );

            { // op_assign_ref
                let mut x = b.clone();
                x ^= &c;
                x &= &a;

                let y = {
                    let mut ab = a.clone();
                    ab &= &b;

                    let mut ac = a.clone();
                    ac &= &c;

                    ab ^= &ac;
                    ab
                };

                prop_assert_eq!(x, y);
            }

            { // op_assign_own
                let mut x = b.clone();
                x ^= c.clone();
                x &= a.clone();

                let y = {
                    let mut ab = a.clone();
                    ab &= b;

                    let mut ac = a;
                    ac &= c;

                    // moves the owned ac on the rhs
                    ab ^= ac;
                    ab
                };

                prop_assert_eq!(x, y);
            }
        }
    }

    // Identity:
    // --------

    proptest! {
        #[test]
        fn the_empty_set_is_the_identity_for_union(a in RoaringBitmap::arbitrary()) {
            prop_assert_eq!(&(&a | &empty_set()), &a);

            #[allow(clippy::redundant_clone)]
            { // op_assign_ref
                let mut x = a.clone();
                x |= &empty_set();

                prop_assert_eq!(x, a.clone());
            }

            { // op_assign_own
                let mut x = a.clone();
                // empty_set() returns an owned empty set
                x |= empty_set();

                prop_assert_eq!(x, a);
            }
        }

        #[test]
        fn the_empty_set_is_the_identity_for_symmetric_difference(a in RoaringBitmap::arbitrary()) {
            prop_assert_eq!(&(&a ^ &empty_set()), &a);

            #[allow(clippy::redundant_clone)]
            { // op_assign_ref
                let mut x = a.clone();
                x ^= &empty_set();

                prop_assert_eq!(x, a.clone());
            }

            { // op_assign_own
                let mut x = a.clone();
                // empty_set() returns an owned empty set
                x ^= empty_set();

                prop_assert_eq!(x, a);
            }
        }
    }

    // Some additional laws for unions and intersections
    // =================================================
    //
    // PROPOSITION 3: For any subsets A and B of a universe set U, the following identities hold:
    //
    // Idempotent laws
    // ---------------

    proptest! {
        #[test]
        fn unions_are_idempotent(a in RoaringBitmap::arbitrary()) {
            prop_assert_eq!(&(&a | &a), &a);

            { // op_assign_ref
                let mut x = a.clone();
                x |= &a;

                prop_assert_eq!(x, a.clone());
            }

            { // op_assign_own
                let mut x = a.clone();
                x |= a.clone();

                prop_assert_eq!(x, a);
            }
        }

        #[test]
        fn intersections_are_idempotent(a in RoaringBitmap::arbitrary()) {
            prop_assert_eq!(&(&a & &a), &a);

            { // op_assign_ref
                let mut x = a.clone();
                x &= &a;

                prop_assert_eq!(x, a.clone());
            }

            { // op_assign_own
                let mut x = a.clone();
                x &= a.clone();

                prop_assert_eq!(x, a);
            }
        }
    }

    //
    // Domination laws
    // ---------------

    proptest! {
        #[test]
        fn empty_set_domination(a in RoaringBitmap::arbitrary()) {
            prop_assert_eq!(&a & &empty_set(), empty_set());

            { // op_assign_ref
                let mut x = a.clone();
                x &= &empty_set();

                prop_assert_eq!(x, empty_set());
            }

            { // op_assign_own
                let mut x = a;
                x &= empty_set();

                prop_assert_eq!(x, empty_set());
            }
        }
    }

    // The algebra of inclusion
    // ========================
    // PROPOSITION 6: If A, B and C are sets then the following hold:
    //
    // Note that for inclusion we do not also assert for the assignment operators
    // Inclusion is the property under test, not the set operation

    proptest! {
        #[test]
        fn reflexivity(a in RoaringBitmap::arbitrary()) {
            prop_assert!(a.is_subset(&a));
        }

        #[test]
        fn antisymmetry(a in RoaringBitmap::arbitrary()) {
            let mut b = a.clone();
            prop_assert_eq!(&a, &b);
            prop_assert!(a.is_subset(&b) && b.is_subset(&a));

            // Flip one bit
            let mut c = RoaringBitmap::new();
            c.insert(0);
            b ^= c;

            prop_assert_ne!(&a, &b);
            prop_assert!(!(a.is_subset(&b) && b.is_subset(&a)));
        }

        #[test]
        fn transitivity(
            a in RoaringBitmap::arbitrary(),
            mut b in RoaringBitmap::arbitrary(),
            mut c in RoaringBitmap::arbitrary()
        ) {
            b |= &a;
            c |= &b;
            // If
            prop_assert!(a.is_subset(&b));
            prop_assert!(b.is_subset(&c));
            // Then
            prop_assert!(a.is_subset(&c));

        }
    }

    // PROPOSITION 7: If A, B and C are subsets of a set S then the following hold:

    proptest! {
        #[test]
        fn existence_of_joins(a in RoaringBitmap::arbitrary(), b in RoaringBitmap::arbitrary()) {
            prop_assert!(a.is_subset(&(&a | &b)));
        }

        #[test]
        fn existence_of_meets(a in RoaringBitmap::arbitrary(), b in RoaringBitmap::arbitrary()) {
            prop_assert!(&(&a & &b).is_subset(&a));
        }
    }

    // PROPOSITION 8: For any two sets A and B, the following are equivalent:

    proptest! {
        #[test]
        fn inclusion_can_be_characterized_by_union_or_inersection(
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            let a = &b - &c;

            prop_assert!(a.is_subset(&b));
            prop_assert_eq!(&(&a & &b), &a);
            prop_assert_eq!(&(&a | &b), &b);
            prop_assert_eq!(&(&a - &b), &empty_set());
        }
    }

    // The algebra of relative complements
    // ===================================
    //
    // PROPOSITION 9: For any universe U and subsets A, B, and C of U,
    // the following identities hold:
    // Note: I dont have good names for these identities. If somebody could give them good names
    // and split each triplet of ref-ref, owned-ref, and owned-owned into a separate test:
    // I will happily buy them a very large but finite number of beers.

    proptest! {
        #[test]
        fn relative_compliments(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            let u = &a | &b | &c;

            prop_assert_eq!(
                &c - (&a & &b),
                (&c - &a) | (&c - &b)
            );

            { // op assign ref
                let mut a_and_b = a.clone();
                a_and_b &= &b;

                let mut c_sub_a = c.clone();
                c_sub_a -= &a;

                let mut c_sub_b = c.clone();
                c_sub_b -= &b;

                let mut x = c.clone();
                x -= &a_and_b;

                let mut y = c_sub_a;
                y |= &c_sub_b;

                prop_assert_eq!(x, y);
            }

            { // op assign own
                let mut a_and_b = a.clone();
                a_and_b &= b.clone();

                let mut c_sub_a = c.clone();
                c_sub_a -= a.clone();

                let mut c_sub_b = c.clone();
                c_sub_b -= b.clone();

                let mut x = c.clone();
                x -= a_and_b;

                let mut y = c_sub_a;
                y |= c_sub_b;

                prop_assert_eq!(x, y);
            }

            prop_assert_eq!(
                &c - (&a | &b),
                (&c - &a) & (&c - &b)
            );

            { // op assign ref
                let mut a_or_b = a.clone();
                a_or_b |= &b;

                let mut c_sub_a = c.clone();
                c_sub_a -= &a;

                let mut c_sub_b = c.clone();
                c_sub_b -= &b;

                let mut x = c.clone();
                x -= &a_or_b;

                let mut y = c_sub_a;
                y &= &c_sub_b;

                prop_assert_eq!(x, y);
            }

            { // op assign own
                let mut a_or_b = a.clone();
                a_or_b |= b.clone();

                let mut c_sub_a = c.clone();
                c_sub_a -= a.clone();

                let mut c_sub_b = c.clone();
                c_sub_b -= b.clone();

                let mut x = c.clone();
                x -= a_or_b;

                let mut y = c_sub_a;
                y &= c_sub_b;

                prop_assert_eq!(x, y);
            }

            prop_assert_eq!(
                &c - (&b - &a),
                (&a & &c) | (&c - &b)
            );

            { // op assign ref
                let mut b_sub_a = b.clone();
                b_sub_a -= &b;

                let mut a_and_c = c.clone();
                a_and_c &= &c;

                let mut c_sub_b = c.clone();
                c_sub_b -= &b;

                let mut x = c.clone();
                x -= &b_sub_a;

                let mut y = a_and_c;
                y |= &c_sub_b;

                prop_assert_eq!(x, y);
            }

            { // op assign own
                let mut b_sub_a = b.clone();
                b_sub_a -= b.clone();

                let mut a_and_c = c.clone();
                a_and_c &= c.clone();

                let mut c_sub_b = c.clone();
                c_sub_b -= b.clone();

                let mut x = c.clone();
                x -= b_sub_a;

                let mut y = a_and_c;
                y |= c_sub_b;

                prop_assert_eq!(x, y);
            }

            {
                let x = (&b - &a) & &c;
                let y = (&b & &c) - &a;
                let z = &b & (&c - &a);

                prop_assert_eq!(&x, &y);
                prop_assert_eq!(&y, &z);
                prop_assert_eq!(&z, &x);
            }

            { // op assign ref
                let mut b_sub_a = b.clone();
                b_sub_a -= &a;

                let mut b_and_c = b.clone();
                b_and_c &= &c;

                let mut c_sub_a = c.clone();
                c_sub_a -= &a;

                let mut x = b_sub_a;
                x &= &c;

                let mut y = b_and_c;
                y -= &a;

                let mut z = c_sub_a;
                z &= &b;

                prop_assert_eq!(&x, &y);
                prop_assert_eq!(&y, &z);
                prop_assert_eq!(&z, &x);
            }

            { // op assign own
                let mut b_sub_a = b.clone();
                b_sub_a -= a.clone();

                let mut b_and_c = b.clone();
                b_and_c &= c.clone();

                let mut c_sub_a = c.clone();
                c_sub_a -= a.clone();

                let mut x = b_sub_a;
                x &= c.clone();

                let mut y = b_and_c;
                y -= a.clone();

                let mut z = c_sub_a;
                z &= b.clone();

                prop_assert_eq!(&x, &y);
                prop_assert_eq!(&y, &z);
                prop_assert_eq!(&z, &x);
            }

            prop_assert_eq!(
                (&b - &a) | &c,
                (&b | &c) - (&a - &c)
            );

            { // op assign ref
                let mut  b_sub_a = b.clone();
                b_sub_a -= &a;

                let mut b_or_c = b.clone();
                b_or_c |= &c;

                let mut a_sub_c = a.clone();
                a_sub_c -= &c;

                let mut x = b_sub_a;
                x |= &c;

                let mut y = b_or_c;
                y -= &a_sub_c;

                prop_assert_eq!(x, y);
            }

            { // op assign own
                let mut  b_sub_a = b.clone();
                b_sub_a -= a.clone();

                let mut b_or_c = b.clone();
                b_or_c |= c.clone();

                let mut a_sub_c = a.clone();
                a_sub_c -= c.clone();

                let mut x = b_sub_a;
                x |= c.clone();

                let mut y = b_or_c;
                y -= a_sub_c;

                prop_assert_eq!(x, y);
            }

            prop_assert_eq!(
                (&b - &a) - &c,
                &b - (&a | &c)
            );

            { // op assign ref
                let mut  b_sub_a = b.clone();
                b_sub_a -= &a;

                let mut a_or_c = a.clone();
                a_or_c |= &c;

                let mut x = b_sub_a;
                x -= &c;

                let mut y = b.clone();
                y -= &a_or_c;

                prop_assert_eq!(x, y);
            }

            { // op assign ref
                let mut  b_sub_a = b.clone();
                b_sub_a -= a.clone();

                let mut a_or_c = a.clone();
                a_or_c |= c.clone();

                let mut x = b_sub_a;
                x -= c;

                let mut y = b;
                y -= a_or_c;

                prop_assert_eq!(x, y);
            }

            prop_assert_eq!(
                &a - &a,
                empty_set()
            );

            { // op assign ref
                let mut x = a.clone();
                x -= &a;

                prop_assert_eq!(x, empty_set());
            }

            { // op assign own
                let mut x = a.clone();
                x -= a.clone();

                prop_assert_eq!(x, empty_set());
            }

             prop_assert_eq!(
                empty_set() - &a,
                empty_set()
            );

            { // op assign ref
            let mut x = empty_set();
            x -= &a;

            prop_assert_eq!(x, empty_set());
        }

        { // op assign own
                let mut x = empty_set();
                x -= a.clone();

                prop_assert_eq!(x, empty_set());
            }

            prop_assert_eq!(
                &a - &u,
                empty_set()
            );

            { // op assign ref
                let mut x = a.clone();
                x -= &u;

                prop_assert_eq!(x, empty_set());
            }

            { // op assign own
                let mut x = a;
                x -= u;

                prop_assert_eq!(x, empty_set());
            }
        }
    }

    // Additional properties of symmetric differences
    // ==============================================
    //

    proptest! {
        #[test]
        fn symmetric_difference_triangle_inequality(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary(),
            c in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(
                &((&a ^ &b) ^ (&b ^ &c)),
                &(&a ^ &c)
            );

            { // op assign ref
                let mut a_xor_b = a.clone();
                a_xor_b ^= &b;

                let mut b_xor_c = b.clone();
                b_xor_c ^= &c;

                let mut a_xor_c = a.clone();
                a_xor_c ^= &c;

                let mut tri = a_xor_b;
                tri ^= &b_xor_c;

                prop_assert_eq!(tri, a_xor_c);
            }

            { // op assign own
                let mut a_xor_b = a.clone();
                a_xor_b ^= b.clone();

                let mut b_xor_c = b;
                b_xor_c ^= c.clone();

                let mut a_xor_c = a;
                a_xor_c ^= c;

                let mut tri = a_xor_b;
                tri ^= b_xor_c;

                prop_assert_eq!(tri, a_xor_c);
            }
        }

        #[test]
        fn symmetric_difference_empty_set_neutral(
            a in RoaringBitmap::arbitrary()
        ) {
            prop_assert_eq!(
                &(&a ^ &empty_set()),
                &a
            );

            { // op assign ref
                let mut x = a.clone();
                x ^= &empty_set();

                prop_assert_eq!(&x, &a);
            }

            { // op assign own
                let mut x = a.clone();
                x ^= empty_set();

                prop_assert_eq!(x, a);
            }
        }

        #[test]
        fn symmetric_difference_inverse_of_itself(
            a in RoaringBitmap::arbitrary()
        ) {

            prop_assert_eq!(
                &(&a ^ &a),
                &empty_set()
            );

            { // op assign ref
                let mut x = a.clone();
                x ^= &a;

                prop_assert_eq!(&x, &empty_set());
            }

            { // op assign own
                let mut x = a.clone();
                x ^= a;

                prop_assert_eq!(x, empty_set());
            }
        }

        #[test]
        fn symmetric_difference_relative_compliments(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary()
        ) {

            prop_assert_eq!(
                &(&a ^ &b),
                &(&(&a - &b) | &(&b - &a))
            );

            { // op assign ref
                let mut x = a.clone();
                x ^= &b;

                let mut a_sub_b = a.clone();
                a_sub_b -= &b;

                let mut b_sub_a = b.clone();
                b_sub_a -= &a;

                let mut y = a_sub_b;
                y |= &b_sub_a;

                prop_assert_eq!(x, y);
            }

            { // op assign own
                let mut x = a.clone();
                x ^= b.clone();

                let mut a_sub_b = a.clone();
                a_sub_b -= b.clone();

                let mut b_sub_a = b.clone();
                b_sub_a -= a.clone();

                let mut y = a_sub_b;
                y |= b_sub_a;

                prop_assert_eq!(x, y);
            }

            prop_assert_eq!(
                &(&a ^ &b),
                &(&(&a | &b) - &(&a & &b))
            );

            { // op assign ref
                let mut x = a.clone();
                x ^= &b;

                let mut a_or_b = a.clone();
                a_or_b |= &b;

                let mut a_and_b = a.clone();
                a_and_b &= &b;

                let mut y = a_or_b;
                y -= &a_and_b;

                prop_assert_eq!(x, y);
            }

            { // op assign own
                let mut x = a.clone();
                x ^= b.clone();

                let mut a_or_b = a.clone();
                a_or_b |= b.clone();

                let mut a_and_b = a;
                a_and_b &= b;

                let mut y = a_or_b;
                y -= a_and_b;

                prop_assert_eq!(x, y);
            }
        }
    }

    fn empty_set() -> RoaringBitmap {
        RoaringBitmap::new()
    }
}
