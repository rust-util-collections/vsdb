# vsdb API Examples

This document provides examples for the public APIs in the `vsdb` crate.

## Vecx

`Vecx` is a vector-like data structure that stores values in a contiguous sequence.

```rust
use vsdb::Vecx;

let mut vec = Vecx::new();

// Push some values
vec.push(&10);
vec.push(&20);
vec.push(&30);

// Get a value
assert_eq!(vec.get(1), Some(20));

// Iterate over the values
for value in vec.iter() {
    println!("{}", value);
}

// Pop a value
assert_eq!(vec.pop(), Some(30));
assert_eq!(vec.len(), 2);
```

### Mapx

`Mapx` is a hash map-like data structure that stores key-value pairs.

```rust
use vsdb::Mapx;

let mut map = Mapx::new();

// Insert some key-value pairs
map.insert(&"key1", &"value1");
map.insert(&"key2", &"value2");

// Get a value
assert_eq!(map.get(&"key1"), Some("value1".to_string()));

// Check if a key exists
assert!(map.contains_key(&"key2"));

// Iterate over the key-value pairs
for (key, value) in map.iter() {
    println!("{}: {}", key, value);
}

// Remove a key-value pair
map.remove(&"key1");
assert_eq!(map.len(), 1);
```

### MapxOrd

`MapxOrd` is a B-tree map-like data structure that stores key-value pairs in a sorted order.

```rust
use vsdb::MapxOrd;

let mut map = MapxOrd::new();

// Insert some key-value pairs
map.insert(&3, &"three");
map.insert(&1, &"one");
map.insert(&2, &"two");

// Get a value
assert_eq!(map.get(&1), Some("one".to_string()));

// Iterate over the key-value pairs in sorted order
for (key, value) in map.iter() {
    println!("{}: {}", key, value);
}

// Get the first and last key-value pairs
assert_eq!(map.first(), Some((1, "one".to_string())));
assert_eq!(map.last(), Some((3, "three".to_string())));
```
