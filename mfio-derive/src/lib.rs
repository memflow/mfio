use proc_macro::TokenStream;
use quote::*;
use syn::*;

#[proc_macro_derive(SyncIoRead)]
pub fn io_read(item: TokenStream) -> TokenStream {
    let st = parse_macro_input!(item as ItemStruct);

    let ident = &st.ident;
    let type_gens = st.generics.split_for_impl().1;
    let impl_bounds = &st.generics.params;
    let where_bounds = st.generics.where_clause.as_ref().map(|v| &v.predicates);

    let impl_comma = if !impl_bounds.trailing_punct() && !impl_bounds.is_empty() {
        Some(token::Comma::default())
    } else {
        None
    };

    quote! {
        impl<#impl_bounds #impl_comma __Pos: 'static> mfio::traits::sync::SyncIoRead<__Pos> for #ident #type_gens where #ident #type_gens: mfio::traits::IoRead<__Pos> + mfio::backend::IoBackend, #where_bounds {}
    }.into()
}

#[proc_macro_derive(SyncIoWrite)]
pub fn io_write(item: TokenStream) -> TokenStream {
    let st = parse_macro_input!(item as ItemStruct);

    let ident = &st.ident;
    let type_gens = st.generics.split_for_impl().1;
    let impl_bounds = &st.generics.params;
    let where_bounds = st.generics.where_clause.as_ref().map(|v| &v.predicates);

    let impl_comma = if !impl_bounds.trailing_punct() && !impl_bounds.is_empty() {
        Some(token::Comma::default())
    } else {
        None
    };

    quote! {
        impl<#impl_bounds #impl_comma __Pos: 'static> mfio::traits::sync::SyncIoWrite<__Pos> for #ident #type_gens where #ident #type_gens: mfio::traits::IoWrite<__Pos> + mfio::backend::IoBackend, #where_bounds {}
    }.into()
}
