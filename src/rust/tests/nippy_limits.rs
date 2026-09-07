use datalevin_codec::nippy::{ErrorKind, Limits, Value, fast_freeze, fast_thaw_with_limits};

fn check_budget(wire: &[u8], expected: &Value, nodes: usize, depth: usize) {
    let limits = Limits {
        max_values: nodes,
        max_allocation_bytes: nodes * std::mem::size_of::<Value>(),
        max_depth: depth,
        ..Limits::default()
    };
    assert_eq!(fast_thaw_with_limits(wire, limits).unwrap(), *expected);
    for insufficient in [
        Limits {
            max_values: nodes - 1,
            ..limits
        },
        Limits {
            max_allocation_bytes: limits.max_allocation_bytes - 1,
            ..limits
        },
        Limits {
            max_depth: depth - 1,
            ..limits
        },
    ] {
        assert_eq!(
            fast_thaw_with_limits(wire, insufficient).unwrap_err().kind,
            ErrorKind::LimitExceeded
        );
    }
}

#[test]
fn small_vectors_obey_exact_node_byte_and_depth_budgets() {
    for length in 0..=4 {
        let value = Value::Vector(vec![Value::Vector(vec![Value::Null; length])]);
        let wire = fast_freeze(&value).unwrap();
        check_budget(&wire, &value, 2 + length, if length == 0 { 1 } else { 2 });
        for prefix in 0..wire.len() {
            assert!(fast_thaw_with_limits(&wire[..prefix], Limits::default()).is_err());
        }
    }
}

#[test]
fn cached_collections_charge_definitions_copies_and_expansions() {
    // Root, cache markers, definition, owned cache copy, and expanded reference.
    let wire = [113, 59, 113, 3, 3, 59];
    let value = Value::Vector(vec![Value::Vector(vec![Value::Null; 2]); 2]);
    check_budget(&wire, &value, 12, 3);
}
