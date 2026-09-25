use std::ops::{Bound, RangeBounds};
use vsdb::{SlotDex64, SlotDex128, slotdex::Order};

#[test]
fn stream_matches_slot_and_key_order_for_all_bounds() {
    let rows = [
        (0, 5),
        (0, 1),
        (3, 2),
        (3, 1),
        (5, 1),
        (u64::MAX, 1),
        (u64::MAX, 0),
    ];
    let ranges = [
        (Bound::Unbounded, Bound::Unbounded),
        (Bound::Included(0), Bound::Included(u64::MAX)),
        (Bound::Excluded(0), Bound::Included(5)),
        (Bound::Included(3), Bound::Excluded(5)),
        (Bound::Unbounded, Bound::Excluded(0)),
        (Bound::Excluded(u64::MAX), Bound::Unbounded),
        (Bound::Included(5), Bound::Included(3)),
        (Bound::Included(u64::MAX), Bound::Included(u64::MAX)),
    ];
    for swap in [false, true] {
        let mut index = SlotDex64::<u64>::new(8, swap).unwrap();
        index.insert_batch(rows).unwrap();
        for order in [Order::Asc, Order::Desc] {
            for range in ranges {
                let mut expected: Vec<_> = rows
                    .iter()
                    .copied()
                    .filter(|(slot, _)| range.contains(slot))
                    .collect();
                expected.sort_by(|a, b| {
                    let slots = a.0.cmp(&b.0);
                    let slots = if order == Order::Desc {
                        slots.reverse()
                    } else {
                        slots
                    };
                    slots.then_with(|| a.1.cmp(&b.1))
                });
                let keys: Vec<_> = expected.into_iter().map(|(_, key)| key).collect();
                assert_eq!(index.iter(range, order).collect::<Vec<_>>(), keys);
                assert_eq!(index.page(range, u16::MAX, 0, order), keys);
            }
        }
    }
}

#[test]
fn stream_crosses_page_limit_and_preserves_reopened_u128_index() {
    let count = u64::from(u16::MAX) + 1;
    for swap in [false, true] {
        let mut index = SlotDex128::<u64>::new(8, swap).unwrap();
        index
            .insert_batch((0..count).map(|key| (u128::MAX, key)))
            .unwrap();
        let bytes = postcard::to_allocvec(&index).unwrap();
        let restored: SlotDex128<u64> = postcard::from_bytes(&bytes).unwrap();
        for order in [Order::Asc, Order::Desc] {
            assert!(restored.iter(u128::MAX.., order).eq(0..count));
            assert_eq!(restored.iter(.., order).count() as u64, count);
            assert_eq!(restored.page(.., u16::MAX, 1, order), vec![count - 1]);
        }
    }
}
