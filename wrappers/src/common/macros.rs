#[macro_export]
macro_rules! define_map_wrapper {
    (
        $(#[$struct_doc:meta])*
        $vis:vis struct $wrapper_name:ident <$($wrapper_generics:tt),*> {
            $inner_vis:vis inner: $inner_type:ty,
            $phantom_field:ident: $phantom_type:ty,
        }
        where $($trait_bounds:tt)+
    ) => {
        $(#[$struct_doc])*
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        #[serde(bound = "")]
        $vis struct $wrapper_name<$($wrapper_generics),*> {
            $inner_vis inner: $inner_type,
            $phantom_field: $phantom_type,
        }

        impl<$($wrapper_generics),*> $wrapper_name<$($wrapper_generics),*>
        where
            $($trait_bounds)+
        {
            /// # Safety
            ///
            /// This function is unsafe because it creates a new wrapper instance that shares the same underlying
            /// data source. The caller must ensure that no write operations occur on the original instance
            /// while the shadow instance exists, as this could lead to data corruption or undefined behavior.
            #[inline(always)]
            pub unsafe fn shadow(&self) -> Self {
                unsafe {
                    Self {
                        inner: self.inner.shadow(),
                        $phantom_field: std::marker::PhantomData,
                    }
                }
            }

            /// # Safety
            ///
            /// This function is unsafe because it deserializes the data structure from a raw byte slice.
            /// The caller must ensure that the provided bytes represent a valid, serialized instance of the
            /// data structure. Providing invalid or malicious data can lead to memory unsafety, panics,
            /// or other undefined behavior.
            #[inline(always)]
            pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
                unsafe {
                    Self {
                        inner: <$inner_type>::from_bytes(s),
                        $phantom_field: std::marker::PhantomData,
                    }
                }
            }

            #[inline(always)]
            pub fn as_bytes(&self) -> &[u8] {
                self.inner.as_bytes()
            }

            #[inline(always)]
            pub fn new() -> Self {
                Self {
                    inner: <$inner_type>::new(),
                    $phantom_field: std::marker::PhantomData,
                }
            }

            #[inline(always)]
            pub fn len(&self) -> usize {
                self.inner.len()
            }

            #[inline(always)]
            pub fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }

            #[inline(always)]
            pub fn clear(&mut self) {
                self.inner.clear();
            }

            #[inline(always)]
            pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
                self.inner.is_the_same_instance(&other_hdr.inner)
            }
        }

        impl<$($wrapper_generics),*> Clone for $wrapper_name<$($wrapper_generics),*>
        {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                    $phantom_field: std::marker::PhantomData,
                }
            }
        }

        impl<$($wrapper_generics),*> Default for $wrapper_name<$($wrapper_generics),*>
        where
            $($trait_bounds)+
        {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}
