#![allow(dead_code)]

pub use kafka_connect_wasm_sdk::*;

// *****************************************************************************
//
// Functions
//
// ******************************************************************************

#[cfg_attr(all(target_arch = "wasm32"), export_name = "to_upper")]
#[no_mangle]
pub extern fn to_upper() {
    let val = get_connect_record_value();
    let res = String::from_utf8(val).unwrap().to_uppercase().as_bytes().to_vec();

    set_connect_record_value(res);
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "value_to_key")]
#[no_mangle]
pub extern fn value_to_key() {
    let val = get_connect_record_value();

    set_connect_record_key(val);
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "header_to_key")]
#[no_mangle]
pub extern fn header_to_key() {
    let val =get_connect_record_header("the-key".to_string());

    set_connect_record_key(val);
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "copy_header")]
#[no_mangle]
pub extern fn copy_header() {
    let val = get_connect_record_header("the-header-in".to_string());

    set_connect_record_header("the-header-out".to_string(), val);
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "transform")]
#[no_mangle]
pub extern fn transform() {
    let mut msg = get_connect_record();
    msg.key = msg.value.clone();
    msg.value = String::from_utf8(msg.value).unwrap().to_uppercase().as_bytes().to_vec();

    set_connect_record(msg);
}

