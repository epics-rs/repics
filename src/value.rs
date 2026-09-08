//! Value conversion at the Python boundary.
//!
//! Rust → Python: scalars become `int` / `float` / `str`; array variants are
//! moved into a 1-D numpy array of the matching dtype (no copy); string
//! arrays become `list[str]`.
//!
//! Python → Rust: a `str` is a string put (the server converts, so menu
//! labels work); `bool`/`int`/`float` become the widest scalar of their
//! kind and `CaChannel::put` converts to the channel's native type; a
//! 1-D numpy array of a supported dtype becomes the matching array
//! variant; a list is a string array if every element is a `str`, else a
//! `float64` array.

use epics_base_rs::types::{EpicsValue, PvString};
use numpy::{PyArray1, PyReadonlyArray1, PyUntypedArrayMethods};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyString};

pub fn pv_string_to_py(py: Python<'_>, s: &PvString) -> PyResult<Py<PyAny>> {
    s.as_str_lossy().into_py_any(py)
}

/// Convert an `EpicsValue` into a Python object. Arrays are moved.
pub fn to_py(py: Python<'_>, v: EpicsValue) -> PyResult<Py<PyAny>> {
    use EpicsValue as V;
    fn arr<T: numpy::Element>(py: Python<'_>, v: Vec<T>) -> PyResult<Py<PyAny>> {
        Ok(PyArray1::from_vec(py, v).into_any().unbind())
    }
    match v {
        V::String(s) => pv_string_to_py(py, &s),
        V::Short(x) => x.into_py_any(py),
        V::Float(x) => x.into_py_any(py),
        V::Enum(x) => x.into_py_any(py),
        V::EnumWithChoices { index, .. } => index.into_py_any(py),
        V::Char(x) => x.into_py_any(py),
        V::Long(x) => x.into_py_any(py),
        V::Double(x) => x.into_py_any(py),
        V::Int64(x) => x.into_py_any(py),
        V::UInt64(x) => x.into_py_any(py),
        V::UShort(x) => x.into_py_any(py),
        V::ULong(x) => x.into_py_any(py),
        V::UChar(x) => x.into_py_any(py),
        V::ShortArray(a) => arr(py, a),
        V::FloatArray(a) => arr(py, a),
        V::EnumArray(a) => arr(py, a),
        V::DoubleArray(a) => arr(py, a),
        V::LongArray(a) => arr(py, a),
        V::CharArray(a) => arr(py, a),
        V::Int64Array(a) => arr(py, a),
        V::UInt64Array(a) => arr(py, a),
        V::UShortArray(a) => arr(py, a),
        V::ULongArray(a) => arr(py, a),
        V::UCharArray(a) => arr(py, a),
        V::StringArray(a) => {
            let items = a
                .iter()
                .map(|s| pv_string_to_py(py, s))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, items)?.into_any().unbind())
        }
    }
}

/// What a Python value asks the channel to send.
pub enum PutRequest {
    /// `CaChannel::put_string` — the server converts.
    Str(String),
    /// `CaChannel::put_string_array`.
    StrArray(Vec<String>),
    /// `CaChannel::put` — converted to the native type by the client.
    Value(EpicsValue),
}

pub fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<PutRequest> {
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(PutRequest::Str(s.to_cow()?.into_owned()));
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(PutRequest::Value(EpicsValue::Long(b.is_true() as i32)));
    }
    if obj.cast::<PyInt>().is_ok() {
        return Ok(PutRequest::Value(EpicsValue::Int64(obj.extract::<i64>()?)));
    }
    if obj.cast::<PyFloat>().is_ok() {
        return Ok(PutRequest::Value(EpicsValue::Double(obj.extract::<f64>()?)));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        if list.len() > 0 && list.iter().all(|e| e.is_instance_of::<PyString>()) {
            return Ok(PutRequest::StrArray(list.extract::<Vec<String>>()?));
        }
        return Ok(PutRequest::Value(EpicsValue::DoubleArray(
            list.extract::<Vec<f64>>()?,
        )));
    }
    if let Some(v) = numpy_to_value(obj)? {
        return Ok(PutRequest::Value(v));
    }
    // numpy scalars (np.float64, np.int32, ...) implement __float__/__index__.
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(PutRequest::Value(EpicsValue::Int64(i)));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(PutRequest::Value(EpicsValue::Double(f)));
    }
    Err(PyTypeError::new_err(format!(
        "cannot put a value of type {}",
        obj.get_type().name()?
    )))
}

fn numpy_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Option<EpicsValue>> {
    macro_rules! try_dtype {
        ($t:ty, $variant:ident) => {
            if let Ok(a) = obj.extract::<PyReadonlyArray1<$t>>() {
                if a.ndim() != 1 {
                    return Err(PyTypeError::new_err("only 1-D arrays can be put"));
                }
                return Ok(Some(EpicsValue::$variant(a.as_slice()?.to_vec())));
            }
        };
    }
    try_dtype!(f64, DoubleArray);
    try_dtype!(f32, FloatArray);
    try_dtype!(i64, Int64Array);
    try_dtype!(i32, LongArray);
    try_dtype!(i16, ShortArray);
    try_dtype!(u8, UCharArray);
    try_dtype!(u16, UShortArray);
    try_dtype!(u32, ULongArray);
    try_dtype!(u64, UInt64Array);
    Ok(None)
}
