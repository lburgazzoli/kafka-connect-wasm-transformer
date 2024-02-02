use std::mem;
use std::slice;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use base64_serde::base64_serde_type;

base64_serde_type!(Base64Standard, base64::engine::general_purpose::STANDARD);

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub headers: HashMap<String, serde_json::Value>,

    pub topic: String,

    #[serde(default)]
    #[serde(with = "Base64Standard")]
    pub key: Vec<u8>,

    #[serde(with = "Base64Standard")]
    pub value: Vec<u8>,
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
// Host Functions and Helpers
//
// ******************************************************************************

extern "C" {
	fn set_value(ptr: *const u8, len: i32);
	fn get_value() -> u64;

	fn set_topic(ptr: *const u8, len: i32);
	fn get_topic() -> u64;

	fn set_key(ptr: *const u8, len: i32);
	fn get_key() -> u64;

	fn set_record(ptr: *const u8, len: i32);
	fn get_record() -> u64;


	fn set_header(ptr: *const u8, len: i32, ptr: *const u8, len: i32);
	fn get_header(ptr: *const u8, len: i32) -> u64;
}

pub fn get_connect_record_value() -> Vec<u8> {
    let ptr_and_len = unsafe {
        get_value()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_connect_record_value(v: Vec<u8>) {
     let out_len = v.len();
     let out_ptr = v.as_ptr();

     unsafe {
        set_value(out_ptr, out_len as i32);
     };
}

pub fn get_connect_record_topic() -> Vec<u8> {
    let ptr_and_len = unsafe {
        get_topic()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_connect_record_topic(v: Vec<u8>) {
     let out_len = v.len();
     let out_ptr = v.as_ptr();

     unsafe {
        set_topic(out_ptr, out_len as i32);
     };
}

pub fn get_connect_record_key() -> Vec<u8> {
    let ptr_and_len = unsafe {
        get_key()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_connect_record_key(v: Vec<u8>) {
     let out_len = v.len();
     let out_ptr = v.as_ptr();

     unsafe {
        set_key(out_ptr, out_len as i32);
     };
}

pub fn get_connect_record() -> Message {
    let ptr_and_len = unsafe {
        get_record()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    let bytes = unsafe {
        slice::from_raw_parts_mut(in_ptr, in_len)
    };

    return serde_json::from_slice(bytes).unwrap();
}

pub fn set_connect_record(msg: Message) {
    let mut out_vec = serde_json::to_vec(&msg).unwrap();
    let out_len = out_vec.len();
    let out_ptr = out_vec.as_mut_ptr();

     unsafe {
        set_record(out_ptr, out_len as i32);
     };
}

pub fn get_connect_record_header(name: String) -> Vec<u8> {
    let mut hn_data = name.into_bytes();
    let hn_len = hn_data.len();
    let hn_ptr = hn_data.as_mut_ptr();

    let ptr_and_len = unsafe {
        get_header(hn_ptr, hn_len as i32)
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_connect_record_header(name: String, val: Vec<u8>) {
    let mut hn_data = name.into_bytes();
    let hn_len = hn_data.len();
    let hn_ptr = hn_data.as_mut_ptr();

    let val_len = val.len();
    let val_ptr = val.as_ptr();

    unsafe {
        set_header(hn_ptr, hn_len as i32, val_ptr, val_len as i32)
    };
}
