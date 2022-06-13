use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Index};

#[proc_macro_derive(Vs)]
pub fn derive_vsmgmt(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(input as DeriveInput);

    // Used in the quasi-quotation below as `#name`.
    let name = input.ident;

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let version_create = gen_version_create(&input.data);
    let version_create_by_branch = gen_version_create_by_branch(&input.data);
    let version_exists = gen_version_exists(&input.data);
    let version_exists_on_branch = gen_version_exists_on_branch(&input.data);
    let version_pop = gen_version_pop(&input.data);
    let version_pop_by_branch = gen_version_pop_by_branch(&input.data);
    let version_rebase = gen_version_rebase(&input.data);
    let version_rebase_by_branch = gen_version_rebase_by_branch(&input.data);

    let branch_create = gen_branch_create(&input.data);
    let branch_create_by_base_branch = gen_branch_create_by_base_branch(&input.data);
    let branch_create_by_base_branch_version =
        gen_branch_create_by_base_branch_version(&input.data);

    let branch_create_without_new_version =
        gen_branch_create_without_new_version(&input.data);
    let branch_create_by_base_branch_without_new_version =
        gen_branch_create_by_base_branch_without_new_version(&input.data);
    let branch_create_by_base_branch_version_without_new_version =
        gen_branch_create_by_base_branch_version_without_new_version(&input.data);

    let branch_exists = gen_branch_exists(&input.data);
    let branch_has_versions = gen_branch_has_versions(&input.data);
    let branch_remove = gen_branch_remove(&input.data);
    let branch_keep_only = gen_branch_keep_only(&input.data);
    let branch_truncate = gen_branch_truncate(&input.data);
    let branch_truncate_to = gen_branch_truncate_to(&input.data);
    let branch_pop_version = gen_branch_pop_version(&input.data);
    let branch_merge_to = gen_branch_merge_to(&input.data);
    let branch_merge_to_force = gen_branch_merge_to_force(&input.data);
    let branch_set_default = gen_branch_set_default(&input.data);
    let prune = gen_prune(&input.data);

    let version_exists_globally = gen_version_exists_globally(&input.data);
    let version_list = gen_version_list(&input.data);
    let version_list_by_branch = gen_version_list_by_branch(&input.data);
    let version_list_globally = gen_version_list_globally(&input.data);
    let version_has_change_set = gen_version_has_change_set(&input.data);
    let version_clean_up_globally = gen_version_clean_up_globally(&input.data);
    let version_revert_globally = gen_version_revert_globally(&input.data);
    let branch_is_empty = gen_branch_is_empty(&input.data);
    let branch_list = gen_branch_list(&input.data);
    let branch_get_default = gen_branch_get_default(&input.data);
    let branch_swap = gen_branch_swap(&input.data);

    let expanded = quote! {
        use ruc::*;
        impl #impl_generics vsdb::VsMgmt for #name #ty_generics #where_clause {
            fn version_create(&mut self, version_name: vsdb::VersionName) -> ruc::Result<()> {
                #version_create
                Ok(())
            }

            fn version_create_by_branch(
                &mut self,
                version_name: vsdb::VersionName,
                branch_name: vsdb::BranchName,
                ) -> ruc::Result<()> {
                #version_create_by_branch
                Ok(())
            }

            fn version_exists(&self, version_name: vsdb::VersionName) -> bool {
                #version_exists
            }

            fn version_exists_on_branch(
                &self,
                version_name: vsdb::VersionName,
                branch_name: vsdb::BranchName,
                ) -> bool {
                #version_exists_on_branch
            }

            fn version_pop(&mut self) -> ruc::Result<()> {
                #version_pop
                Ok(())
            }

            fn version_pop_by_branch(&mut self, branch_name: vsdb::BranchName) -> ruc::Result<()> {
                #version_pop_by_branch
                Ok(())
            }

            unsafe fn version_rebase(&mut self, base_version: vsdb::VersionName) -> ruc::Result<()> {
                #version_rebase
                Ok(())
            }

            unsafe fn version_rebase_by_branch(
                &mut self,
                base_version: vsdb::VersionName,
                branch_name: vsdb::BranchName
            ) -> ruc::Result<()> {
                #version_rebase_by_branch
                Ok(())
            }

            fn branch_create(
                &mut self,
                branch_name: vsdb::BranchName,
                version_name: vsdb::VersionName,
                force: bool
            ) -> ruc::Result<()> {
                #branch_create
                Ok(())
            }

            fn branch_create_by_base_branch(
                &mut self,
                branch_name: vsdb::BranchName,
                version_name: vsdb::VersionName,
                base_branch_name: vsdb::ParentBranchName,
                force: bool,
            ) -> ruc::Result<()> {
                #branch_create_by_base_branch
                Ok(())
            }

            fn branch_create_by_base_branch_version(
                &mut self,
                branch_name: vsdb::BranchName,
                version_name: vsdb::VersionName,
                base_branch_name: vsdb::ParentBranchName,
                base_version_name: vsdb::VersionName,
                force: bool
            ) -> ruc::Result<()> {
                #branch_create_by_base_branch_version
                Ok(())
            }

            unsafe fn branch_create_without_new_version(
                &mut self,
                branch_name: vsdb::BranchName,
                force: bool
            ) -> ruc::Result<()> {
                #branch_create_without_new_version
                Ok(())
            }

            unsafe fn branch_create_by_base_branch_without_new_version(
                &mut self,
                branch_name: vsdb::BranchName,
                base_branch_name: vsdb::ParentBranchName,
                force: bool
            ) -> ruc::Result<()> {
                #branch_create_by_base_branch_without_new_version
                Ok(())
            }

            unsafe fn branch_create_by_base_branch_version_without_new_version(
                &mut self,
                branch_name: vsdb::BranchName,
                base_branch_name: vsdb::ParentBranchName,
                base_version_name: vsdb::VersionName,
                force: bool
            ) -> ruc::Result<()> {
                #branch_create_by_base_branch_version_without_new_version
                Ok(())
            }

            fn branch_exists(&self, branch_name: vsdb::BranchName) -> bool {
                #branch_exists
            }

            fn branch_has_versions(&self, branch_name: vsdb::BranchName) -> bool {
                #branch_has_versions
            }

            fn branch_remove(&mut self, branch_name: vsdb::BranchName) -> ruc::Result<()> {
                #branch_remove
                Ok(())
            }

            fn branch_keep_only(&mut self, branch_names: &[vsdb::BranchName]) -> ruc::Result<()> {
                #branch_keep_only
                Ok(())
            }

            fn branch_truncate(&mut self, branch_name: vsdb::BranchName) -> ruc::Result<()> {
                #branch_truncate
                Ok(())
            }

            fn branch_truncate_to(
                &mut self,
                branch_name: vsdb::BranchName,
                last_version_name: vsdb::VersionName,
            ) -> ruc::Result<()> {
                #branch_truncate_to
                Ok(())
            }

            fn branch_pop_version(&mut self, branch_name: vsdb::BranchName) -> ruc::Result<()> {
                #branch_pop_version
                Ok(())
            }

            fn branch_merge_to(
                &mut self,
                branch_name: vsdb::BranchName,
                target_branch_name: vsdb::BranchName
            ) -> ruc::Result<()> {
                #branch_merge_to
                Ok(())
            }

            unsafe fn branch_merge_to_force(
                &mut self,
                branch_name: vsdb::BranchName,
                target_branch_name: vsdb::BranchName
            ) -> ruc::Result<()> {
                #branch_merge_to_force
                Ok(())
            }

            fn branch_set_default(&mut self, branch_name: vsdb::BranchName) -> ruc::Result<()> {
                #branch_set_default
                Ok(())
            }

            fn prune(&mut self, reserved_ver_num: Option<usize>) -> ruc::Result<()> {
                #prune
                Ok(())
            }

            fn version_exists_globally(&self, version_name: vsdb::VersionName) -> bool {
                #version_exists_globally
            }

            fn version_list(&self) -> ruc::Result<Vec<vsdb::VersionNameOwned>> {
                let guard_default: Vec<vsdb::VersionNameOwned> = Default::default();
                let mut guard: Vec<vsdb::VersionNameOwned> = Default::default();
                #version_list
                Ok(guard)
            }

            fn version_list_by_branch(&self, branch_name: vsdb::BranchName)
                -> ruc::Result<Vec<vsdb::VersionNameOwned>> {

                let guard_default: Vec<vsdb::VersionNameOwned> = Default::default();
                let mut guard: Vec<vsdb::VersionNameOwned> = Default::default();
                #version_list_by_branch
                Ok(guard)
            }

            fn version_list_globally(&self) -> Vec<vsdb::VersionNameOwned> {
                let guard_default: Vec<vsdb::VersionNameOwned> = Default::default();
                let mut guard: Vec<vsdb::VersionNameOwned> = Default::default();
                #version_list_globally
                guard
            }

            fn version_has_change_set(&self, version_name: vsdb::VersionName) -> ruc::Result<bool> {
                #version_has_change_set
                Ok(true)
            }

            fn version_clean_up_globally(&mut self) -> ruc::Result<()> {
                #version_clean_up_globally
                Ok(())
            }

            unsafe fn version_revert_globally(&mut self, version_name: vsdb::VersionName) -> ruc::Result<()> {
                #version_revert_globally
                Ok(())
            }

            fn branch_is_empty(&self, branch_name: vsdb::BranchName) -> ruc::Result<bool> {
                #branch_is_empty
                Ok(true)
            }

            fn branch_list(&self) -> Vec<vsdb::BranchNameOwned> {
                let guard_default: Vec<vsdb::BranchNameOwned> = Default::default();
                let mut guard: Vec<vsdb::BranchNameOwned> = Default::default();
                #branch_list
                guard
            }

            fn branch_get_default(&self) -> vsdb::BranchNameOwned {
                let guard_default = vsdb::BranchNameOwned::default();
                let mut guard = vsdb::BranchNameOwned::default();
                #branch_get_default
                guard
            }

            unsafe fn branch_swap(
                &mut self,
                br1: vsdb::BranchName,
                br2: vsdb::BranchName
            ) -> ruc::Result<()> {
                #branch_swap
                Ok(())
            }
        }
    };

    // Hand the output tokens back to the compiler.
    proc_macro::TokenStream::from(expanded)
}

fn gen_version_create(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_create(&mut self.#id, version_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_create(&mut self.#id, version_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_create_by_branch(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                        let id = &f.ident;
                        quote_spanned! {f.span()=>
                            vsdb::VsMgmt::version_create_by_branch(&mut self.#id, version_name, branch_name)
                                .c(d!())?;
                        }
                    });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                        let id = Index::from(i);
                        quote_spanned! {f.span()=>
                            vsdb::VsMgmt::version_create_by_branch(&mut self.#id, version_name, branch_name)
                                .c(d!())?;
                        }
                    });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_exists(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_exists(&self.#id, version_name) &&
                    }
                });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_exists(&self.#id, version_name) &&
                    }
                });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_exists_on_branch(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                        let id = &f.ident;
                        quote_spanned! {f.span()=>
                            vsdb::VsMgmt::version_exists_on_branch(&self.#id, version_name, branch_name) &&
                        }
                    });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                        let id = Index::from(i);
                        quote_spanned! {f.span()=>
                            vsdb::VsMgmt::version_exists_on_branch(&self.#id, version_name, branch_name) &&
                        }
                    });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_pop(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_pop(&mut self.#id).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_pop(&mut self.#id).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_pop_by_branch(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_pop_by_branch(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_pop_by_branch(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_rebase(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_rebase(&mut self.#id, base_version).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_rebase(&mut self.#id, base_version).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_rebase_by_branch(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_rebase_by_branch(&mut self.#id, base_version, branch_name)
                            .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_rebase_by_branch(&mut self.#id, base_version, branch_name)
                            .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_create(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create(&mut self.#id, branch_name, version_name, force)
                            .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create(&mut self.#id, branch_name, version_name, force)
                            .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_create_by_base_branch(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create_by_base_branch(
                            &mut self.#id,
                            branch_name,
                            version_name,
                            base_branch_name,
                            force
                        )
                        .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create_by_base_branch(
                            &mut self.#id,
                            branch_name,
                            version_name,
                            base_branch_name,
                            force
                        )
                        .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_create_by_base_branch_version(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create_by_base_branch_version(
                            &mut self.#id,
                            branch_name,
                            version_name,
                            base_branch_name,
                            base_version_name,
                            force
                        ).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create_by_base_branch_version(
                            &mut self.#id,
                            branch_name,
                            version_name,
                            base_branch_name,
                            base_version_name,
                            force
                        )
                        .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_create_without_new_version(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create_without_new_version(&mut self.#id, branch_name, force)
                            .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create_without_new_version(&mut self.#id, branch_name, force)
                            .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_create_by_base_branch_without_new_version(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create_by_base_branch_without_new_version(
                            &mut self.#id,
                            branch_name,
                            base_branch_name,
                            force
                        )
                        .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_create_by_base_branch_without_new_version(
                            &mut self.#id,
                            branch_name,
                            base_branch_name,
                            force
                        )
                        .c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_create_by_base_branch_version_without_new_version(
    data: &Data,
) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                        let id = &f.ident;
                        quote_spanned! {f.span()=>
                            vsdb::VsMgmt::branch_create_by_base_branch_version_without_new_version(
                                &mut self.#id,
                                branch_name,
                                base_branch_name,
                                base_version_name,
                                force
                            )
                            .c(d!())?;
                        }
                    });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                        let id = Index::from(i);
                        quote_spanned! {f.span()=>
                            vsdb::VsMgmt::branch_create_by_base_branch_version_without_new_version(
                                &mut self.#id,
                                branch_name,
                                base_branch_name,
                                base_version_name,
                                force
                            )
                            .c(d!())?;
                        }
                    });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_exists(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_exists(&self.#id, branch_name) &&
                    }
                });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_exists(&self.#id, branch_name) &&
                    }
                });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}
fn gen_branch_has_versions(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_has_versions(&self.#id, branch_name) &&
                    }
                });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_has_versions(&self.#id, branch_name) &&
                    }
                });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_remove(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_remove(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_remove(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_keep_only(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_keep_only(&mut self.#id, branch_names).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_keep_only(&mut self.#id, branch_names).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_truncate(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_truncate(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_truncate(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_truncate_to(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                        let id = &f.ident;
                        quote_spanned! {f.span()=>
                            vsdb::VsMgmt::branch_truncate_to(&mut self.#id, branch_name, last_version_name).c(d!())?;
                        }
                    });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                        let id = Index::from(i);
                        quote_spanned! {f.span()=>
                            vsdb::VsMgmt::branch_truncate_to(&mut self.#id, branch_name, last_version_name).c(d!())?;
                        }
                    });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_pop_version(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_pop_version(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_pop_version(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_merge_to(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_merge_to(&mut self.#id, branch_name, target_branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_merge_to(&mut self.#id, branch_name, target_branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_merge_to_force(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_merge_to_force(&mut self.#id, branch_name, target_branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_merge_to_force(&mut self.#id, branch_name, target_branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_set_default(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_set_default(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_set_default(&mut self.#id, branch_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_prune(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::prune(&mut self.#id, reserved_ver_num).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::prune(&mut self.#id, reserved_ver_num).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_exists_globally(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_exists_globally(&self.#id, version_name) &&
                    }
                });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_exists_globally(&self.#id, version_name) &&
                    }
                });
                quote! {
                    #(#recurse)* true
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_list(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        if guard == guard_default {
                            guard = vsdb::VsMgmt::version_list(&self.#id).c(d!())?;
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        if guard == guard_default {
                            guard = vsdb::VsMgmt::version_list(&self.#id).c(d!())?;
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_list_by_branch(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        if guard == guard_default {
                            guard = vsdb::VsMgmt::version_list_by_branch(&self.#id, branch_name).c(d!())?;
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        if guard == guard_default {
                            guard = vsdb::VsMgmt::version_list_by_branch(&self.#id, branch_name).c(d!())?;
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_list_globally(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        if guard == guard_default {
                            guard = vsdb::VsMgmt::version_list_globally(&self.#id);
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        if guard == guard_default {
                            guard = vsdb::VsMgmt::version_list_globally(&self.#id);
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_has_change_set(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        if !vsdb::VsMgmt::version_has_change_set(&self.#id, version_name)? { return Ok(false); }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        if !vsdb::VsMgmt::version_has_change_set(&self.#id, version_name)? { return Ok(false); }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_clean_up_globally(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_clean_up_globally(&mut self.#id).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_clean_up_globally(&mut self.#id).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_version_revert_globally(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_revert_globally(&mut self.#id, version_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::version_revert_globally(&mut self.#id, version_name).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_is_empty(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        if !vsdb::VsMgmt::branch_is_empty(&self.#id, branch_name).c(d!())? { return Ok(false); }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        if !vsdb::VsMgmt::branch_is_empty(&self.#id, branch_name).c(d!())? { return Ok(false); }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_list(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        if guard == guard_default {
                            guard = vsdb::VsMgmt::branch_list(&self.#id);
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        if guard == guard_default {
                            guard = vsdb::VsMgmt::branch_list(&self.#id);
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_get_default(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        let new = vsdb::VsMgmt::branch_get_default(&self.#id);
                        if guard_default != new {
                            if guard_default == guard {
                                guard = new;
                            } else {
                                assert_eq!(guard, new);
                            }
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        let new = vsdb::VsMgmt::branch_get_default(&self.#id);
                        if guard_default != new {
                            if guard_default == guard {
                                guard = new;
                            } else {
                                assert_eq!(guard, new);
                            }
                        }
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}

fn gen_branch_swap(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let id = &f.ident;
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_swap(&mut self.#id, br1, br2).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(ref fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let id = Index::from(i);
                    quote_spanned! {f.span()=>
                        vsdb::VsMgmt::branch_swap(&mut self.#id, br1, br2).c(d!())?;
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => todo!(),
        },
        Data::Enum(_) | Data::Union(_) => todo!(),
    }
}
