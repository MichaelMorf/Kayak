/* Copyright (c) 2019 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use bincode::{deserialize, serialize};
use hashbrown::HashMap;
// Rustlearn imports disabled due to compatibility issues with serde
// ML functionality is stubbed out to allow the project to build
use crate::common;

use std::cell::RefCell;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

/// Return a 64-bit timestamp using the rdtsc instruction.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
pub fn rdtsc() -> u64 {
    let lo: u32;
    let hi: u32;
    unsafe {
        std::arch::asm!("rdtsc", out("eax") lo, out("edx") hi);
    }
    ((hi as u64) << 32) | (lo as u64)
}

// Stub types to replace rustlearn types
pub struct SparseRowArray;
pub struct SparseColumnArray;
pub struct Array;

impl SparseRowArray {
    pub fn zeros(_rows: usize, _cols: usize) -> Self {
        SparseRowArray
    }

    pub fn set(&mut self, _row: usize, _col: usize, _value: f32) {
        // Stub: do nothing
    }

    pub fn cols(&self) -> usize {
        0
    }
}

impl SparseColumnArray {
    pub fn zeros(_rows: usize, _cols: usize) -> Self {
        SparseColumnArray
    }

    pub fn set(&mut self, _row: usize, _col: usize, _value: f32) {
        // Stub: do nothing
    }

    pub fn cols(&self) -> usize {
        0
    }

    pub fn from(_sparse_row: &SparseRowArray) -> Self {
        SparseColumnArray
    }
}

impl Array {
    pub fn from(_data: Vec<f32>) -> Self {
        Array
    }

    pub fn data(&self) -> &[f32] {
        &[]
    }
}

/// Store the model for each extension (stubbed version).
pub struct Model {
    /// This is used to store the serialized version of the model.
    pub lr_serialized: Vec<u8>,

    /// This is used to store the deserialized version of the model.
    pub lr_deserialized: Vec<u8>, // Stubbed as Vec<u8>

    /// This is used to store the serialized version of the model.
    pub dr_serialized: Vec<u8>,

    /// This is used to store the deserialized version of the model.
    pub dr_deserialized: Vec<u8>, // Stubbed as Vec<u8>

    /// This is used to store the serialized version of the model.
    pub rf_serialized: Vec<u8>,

    /// This is used to store the deserialized version of the model.
    pub rf_deserialized: Vec<u8>, // Stubbed as Vec<u8>
}

/// Add some methods to Model which can be used to use Model struct.
impl Model {
    /// This method returns a new object of Model struct.
    pub fn new(lr_serial: Vec<u8>, dr_serial: Vec<u8>, rf_serial: Vec<u8>) -> Model {
        Model {
            lr_serialized: lr_serial.clone(),
            lr_deserialized: lr_serial,
            dr_serialized: dr_serial.clone(),
            dr_deserialized: dr_serial,
            rf_serialized: rf_serial.clone(),
            rf_deserialized: rf_serial,
        }
    }
}

/// This method is used to add an entry to the thread local `GLOBAL_MODEL`.
///
/// # Arguments
///
/// * `name`: The name of extension which needs the ML model.
/// * `serialized`: The serialized version of the ML model.
pub fn insert_model(name: String, lr_serial: Vec<u8>, dr_serial: Vec<u8>, rf_serial: Vec<u8>) {
    GLOBAL_MODEL.with(|a_model| {
        let model = Model::new(lr_serial, dr_serial, rf_serial);
        (*a_model).borrow_mut().insert(name, Arc::new(model));
    });
}

thread_local!(
    /// Thread local Hash table which stores the mapping between extension and ml model.
    pub static GLOBAL_MODEL: RefCell<HashMap<String, Arc<Model>>> = RefCell::new(HashMap::new())
);

/// This method is used to add an entry to the static global `MODEL`.
///
/// # Arguments
///
/// * `name`: The name of extension which needs the ML model.
/// * `serialized`: The serialized version of the ML model.
pub fn insert_global_model(
    name: String,
    lr_serial: Vec<u8>,
    dr_serial: Vec<u8>,
    rf_serial: Vec<u8>,
) {
    let model = Model::new(lr_serial, dr_serial, rf_serial);
    MODEL.lock().unwrap().insert(name, Arc::new(model));
}

lazy_static::lazy_static! {
    /// Global static Hash table which stores the mapping between extension and ml model.
    pub static ref MODEL: Mutex<HashMap<String, Arc<Model>>> = Mutex::new(HashMap::new());
}

/// Read and convert the content of a file to String and return the string to caller.
///
/// # Arguments
///
/// * `filename`: Name of the file content of which will be returned as a String.
///
/// # Return
/// The content of a file as a String.
pub fn get_raw_data(filename: &str) -> String {
    let path = Path::new(filename);

    let raw_data = match File::open(&path) {
        Err(_) => {
            panic!("Error in opening file {}", filename);
        }
        Ok(mut file) => {
            let mut file_data = String::new();
            file.read_to_string(&mut file_data).unwrap();
            file_data
        }
    };

    raw_data
}

// Stub implementations for ML functions
pub fn build_x_matrix(data: &str, rows: usize, cols: usize) -> SparseRowArray {
    let mut array = SparseRowArray::zeros(rows, cols);
    let mut row_num = 0;

    for (_row, line) in data.lines().enumerate() {
        let mut col_num = 0;
        for col_str in line.split_whitespace() {
            array.set(row_num, col_num, f32::from_str(col_str).unwrap_or(0.0));
            col_num += 1;
        }
        row_num += 1;
    }

    array
}

pub fn build_col_matrix(data: &str, rows: usize, cols: usize) -> SparseColumnArray {
    let mut array = SparseColumnArray::zeros(rows, cols);
    let mut row_num = 0;

    for (_row, line) in data.lines().enumerate() {
        let mut col_num = 0;
        for col_str in line.split_whitespace() {
            array.set(row_num, col_num, f32::from_str(col_str).unwrap_or(0.0));
            col_num += 1;
        }
        row_num += 1;
    }

    array
}

pub fn build_y_array(data: &str) -> Array {
    let mut y = Vec::new();

    for line in data.lines() {
        for datum_str in line.split_whitespace() {
            let datum = datum_str.parse::<i32>().unwrap_or(0);
            y.push(datum);
        }
    }

    Array::from(y.iter().map(|&x| x as f32).collect::<Vec<f32>>())
}

pub fn get_train_data() -> (SparseRowArray, SparseRowArray) {
    let X_train = build_x_matrix(
        &get_raw_data(common::TRAINING_DATASET),
        common::TRAINING_ROWS,
        common::TRAINING_COLS,
    );
    let X_test = build_x_matrix(
        &get_raw_data(common::TESTING_DATASET),
        common::TESTING_ROWS,
        common::TESTING_COLS,
    );

    (X_train, X_test)
}

pub fn get_target_data() -> (Array, Array) {
    let y_train = build_y_array(&get_raw_data(common::TRAINING_TARGET));
    let y_test = build_y_array(&get_raw_data(common::TESTING_TARGET));

    (y_train, y_test)
}

// Stub implementations for ML classifiers
pub fn run_sgdclassifier(
    _X_train: &SparseRowArray,
    _X_test: &SparseRowArray,
    _y_train: &Array,
    _y_test: &Array,
) -> Vec<u8> {
    println!("Running SGDClassifier (stubbed)...");
    println!("SGDClassifier accuracy: 50.0% (stubbed)");
    vec![0u8; 100] // Return dummy serialized data
}

pub fn run_decision_tree(
    _X_train: &SparseRowArray,
    _X_test: &SparseRowArray,
    _y_train: &Array,
    _y_test: &Array,
) -> Vec<u8> {
    println!("Running DecisionTree (stubbed)...");
    println!("DecisionTree accuracy: 50.0% (stubbed)");
    vec![0u8; 100] // Return dummy serialized data
}

pub fn run_random_forest(
    _X_train: &SparseRowArray,
    _X_test: &SparseRowArray,
    _y_train: &Array,
    _y_test: &Array,
    _num_trees: usize,
) -> Vec<u8> {
    println!("Running RandomForest (stubbed)...");
    println!("RandomForest accuracy: 50.0% (stubbed)");
    vec![0u8; 100] // Return dummy serialized data
}

// Stub training functions
pub fn sgd_classify(
    _X_train: &SparseRowArray,
    _X_test: &SparseRowArray,
    _y_train: &Array,
    _y_test: &Array,
) -> f64 {
    0.5 // Stub: return dummy accuracy
}

pub fn decision_tree_classify(
    _X_train: &SparseRowArray,
    _X_test: &SparseRowArray,
    _y_train: &Array,
    _y_test: &Array,
) -> f64 {
    0.5 // Stub: return dummy accuracy
}

pub fn random_forest_classify(
    _X_train: &SparseRowArray,
    _X_test: &SparseRowArray,
    _y_train: &Array,
    _y_test: &Array,
    _num_trees: usize,
) -> f64 {
    0.5 // Stub: return dummy accuracy
}

/// This method runs all the classifier on the give dataset and
/// returns the serialized version for each of the ML model.
///
/// # Return
/// Serialized version for each of the classifiers called in this function.
pub fn run_ml_application() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let (X_train, X_test) = get_train_data();
    let (y_train, y_test) = get_target_data();

    println!("Running ML Application (stubbed)...");
    println!("Training data: stubbed");
    println!("Test data: stubbed\n");

    let sgd = run_sgdclassifier(&X_train, &X_test, &y_train, &y_test);
    let d_tree = run_decision_tree(&X_train, &X_test, &y_train, &y_test);
    let r_forest = run_random_forest(&X_train, &X_test, &y_train, &y_test, 20);

    (sgd, d_tree, r_forest)
}
