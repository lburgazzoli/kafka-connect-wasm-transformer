use std::mem;
use std::slice;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use base64_serde::base64_serde_type;

base64_serde_type!(Base64Standard, base64::engine::general_purpose::STANDARD);

#[derive(Serialize, Deserialize)]
struct Message {
    headers: HashMap<String, serde_json::Value>,

    topic: String,

    #[serde(default)]
    #[serde(with = "Base64Standard")]
    key: Vec<u8>,

    #[serde(with = "Base64Standard")]
    value: Vec<u8>,
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "alloc")]
#[no_mangle]
pub extern "C" fn alloc(size: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();

    // tell Rust not to clean this up
    mem::forget(buf);

    ptr
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "dealloc")]
#[no_mangle]
pub unsafe extern "C" fn dealloc(ptr: &mut u8, len: i32) {
    // Retakes the pointer which allows its memory to be freed.
    let _ = Vec::from_raw_parts(ptr, 0, len as usize);
}

// *****************************************************************************
//
// Classic
//
// ******************************************************************************


#[cfg_attr(all(target_arch = "wasm32"), export_name = "transform")]
#[no_mangle]
pub extern fn transform(ptr: u32, len: u32) -> u64 {
    let bytes = unsafe {
        slice::from_raw_parts_mut(
            ptr as *mut u8,
            len as usize)
    };

    let mut msg: Message = serde_json::from_slice(bytes).unwrap();
    msg.value = String::from_utf8(msg.value).unwrap().to_uppercase().as_bytes().to_vec();

    let out_vec = serde_json::to_vec(&msg).unwrap();
    let out_len = out_vec.len();
    let out_ptr = alloc(out_len as u32);

    unsafe {
        std::ptr::copy_nonoverlapping(
            out_vec.as_ptr(),
            out_ptr,
            out_len as usize)
    };

    return ((out_ptr as u64) << 32) | out_len as u64;
}

// *****************************************************************************
//
// With Host Functions
//
// ******************************************************************************

extern "C" {
	fn set_value(ptr: *const u8, len: i32);
	fn get_value() -> u64;
}

pub fn do_get_value() -> Vec<u8> {
    let ptr_and_len = unsafe {
        get_value()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}


pub fn do_set_value(v: Vec<u8>) {
     let out_len = v.len();
     let out_ptr = v.as_ptr();

     unsafe {
        set_value(out_ptr, out_len as i32);
     };
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "to_upper")]
#[no_mangle]
pub extern fn to_upper() {
    let val = do_get_value();
    let _res = String::from_utf8(val).unwrap().to_uppercase().as_bytes().to_vec();

    //do_set_value(res);
}


