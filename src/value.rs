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

use std::ffi::CString;

use epics_base_rs::types::{EpicsValue, PvString};
use numpy::ndarray::ArrayView1;
use numpy::{Element, PyArray1, PyArrayMethods, PyReadonlyArray1, PyUntypedArrayMethods};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyCapsule, PyFloat, PyInt, PyList, PyString};

pub fn pv_string_to_py(py: Python<'_>, s: &PvString) -> PyResult<Py<PyAny>> {
    s.as_str_lossy().into_py_any(py)
}

/// A read-only numpy array over `data`'s elements, whose base object is a
/// capsule holding a clone of `data` so the shared buffer outlives the
/// array. No element is copied; this is how every array, CA or PVA,
/// crosses into Python.
pub(crate) fn shared_to_numpy<T, A>(py: Python<'_>, data: &A) -> PyResult<Py<PyAny>>
where
    T: Element + Copy,
    A: AsRef<[T]> + Clone + Send + 'static,
{
    let keep = data.clone();
    let capsule = PyCapsule::new(py, keep, Some(CString::new("repics.array").unwrap()))?;
    let view = ArrayView1::from(data.as_ref());
    // SAFETY: `data` and its clone in the capsule read one buffer behind an
    // `Arc` that is never reallocated or written while shared, so the
    // elements outlive the numpy array and never move under it.
    let arr = unsafe { PyArray1::<T>::borrow_from_array(&view, capsule.into_any()) };
    let ro = arr.readwrite().make_nonwriteable();
    let out: Bound<'_, PyAny> = (*ro).clone().into_any();
    Ok(out.unbind())
}

/// Convert an `EpicsValue` into a Python object. An array is a read-only
/// numpy view over the `SharedArray` the client decoded into, the buffer a
/// monitor's snapshots and the server's stored value share.
pub fn to_py(py: Python<'_>, v: EpicsValue) -> PyResult<Py<PyAny>> {
    use EpicsValue as V;
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
        V::ShortArray(a) => shared_to_numpy(py, &a),
        V::FloatArray(a) => shared_to_numpy(py, &a),
        V::EnumArray(a) => shared_to_numpy(py, &a),
        V::DoubleArray(a) => shared_to_numpy(py, &a),
        V::LongArray(a) => shared_to_numpy(py, &a),
        V::CharArray(a) => shared_to_numpy(py, &a),
        V::Int64Array(a) => shared_to_numpy(py, &a),
        V::UInt64Array(a) => shared_to_numpy(py, &a),
        V::UShortArray(a) => shared_to_numpy(py, &a),
        V::ULongArray(a) => shared_to_numpy(py, &a),
        V::UCharArray(a) => shared_to_numpy(py, &a),
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
            list.extract::<Vec<f64>>()?.into(),
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
                return Ok(Some(EpicsValue::$variant(a.as_slice()?.to_vec().into())));
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
