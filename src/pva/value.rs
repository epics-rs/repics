//! `Type` and `Value`: the p4p-shaped view of a pvData structure.
//!
//! A `Value` is a handle onto one shared root (`FieldDesc` + `PvField` +
//! changed `BitSet`) plus a dotted prefix; indexing a sub-structure returns
//! another handle on the same root, so `v['alarm']['severity'] = 1` marks
//! the root's bit for `alarm.severity`, exactly as p4p's sub-Value views do.
//! Values nested inside unions, variants and structure arrays are not
//! path-addressable and come back as detached copies.
//!
//! Scalar arrays cross the boundary without copying: the numpy array is
//! built over the `Arc<[T]>` the wire decoder produced, with a capsule
//! holding a clone of that `Arc` as the array's base object.

use std::collections::BTreeSet;
use std::ffi::CString;
use std::sync::{Arc, Mutex, MutexGuard};

use epics_pva_rs::proto::BitSet;
use epics_pva_rs::pvdata::encode::default_value_for;
use epics_pva_rs::pvdata::render_value;
use epics_pva_rs::pvdata::{
    FieldDesc, PvField, ScalarType, ScalarValue, TypedScalarArray, UnionItem, VariantValue,
};
use numpy::ndarray::ArrayView1;
use numpy::{Element, PyArray1, PyArrayMethods, PyReadonlyArray1};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyAttributeError, PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyCapsule, PyDict, PyFloat, PyInt, PyList, PySet, PyString, PyTuple};

// ---------------------------------------------------------------------------
// Descriptor helpers
// ---------------------------------------------------------------------------

fn join(prefix: &str, key: &str) -> String {
    match (prefix.is_empty(), key.is_empty()) {
        (true, _) => key.to_string(),
        (_, true) => prefix.to_string(),
        _ => format!("{prefix}.{key}"),
    }
}

/// Descend `desc` through structure members only.
pub fn desc_at<'a>(desc: &'a FieldDesc, path: &str) -> Option<&'a FieldDesc> {
    let mut cur = desc;
    for seg in path.split('.').filter(|s| !s.is_empty()) {
        match cur {
            FieldDesc::Structure { fields, .. } => cur = &fields.iter().find(|(n, _)| n == seg)?.1,
            _ => return None,
        }
    }
    Some(cur)
}

pub fn field_at<'a>(field: &'a PvField, path: &str) -> Option<&'a PvField> {
    let mut cur = field;
    for seg in path.split('.').filter(|s| !s.is_empty()) {
        match cur {
            PvField::Structure(s) => cur = s.get_field(seg)?,
            _ => return None,
        }
    }
    Some(cur)
}

fn field_at_mut<'a>(field: &'a mut PvField, path: &str) -> Option<&'a mut PvField> {
    let mut cur = field;
    for seg in path.split('.').filter(|s| !s.is_empty()) {
        match cur {
            PvField::Structure(s) => cur = s.get_field_mut(seg)?,
            _ => return None,
        }
    }
    Some(cur)
}

/// `(name, child descriptor, bit index)` for each member of a structure
/// whose own bit is `bit`.
fn children(desc: &FieldDesc, bit: usize) -> Vec<(&str, &FieldDesc, usize)> {
    let FieldDesc::Structure { fields, .. } = desc else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(fields.len());
    let mut b = bit + 1;
    for (name, child) in fields {
        out.push((name.as_str(), child, b));
        b += child.total_bits();
    }
    out
}

fn scalar_code(st: ScalarType) -> &'static str {
    match st {
        ScalarType::Boolean => "?",
        ScalarType::Byte => "b",
        ScalarType::UByte => "B",
        ScalarType::Short => "h",
        ScalarType::UShort => "H",
        ScalarType::Int => "i",
        ScalarType::UInt => "I",
        ScalarType::Long => "l",
        ScalarType::ULong => "L",
        ScalarType::Float => "f",
        ScalarType::Double => "d",
        ScalarType::String => "s",
    }
}

fn scalar_from_code(c: char) -> Option<ScalarType> {
    Some(match c {
        '?' => ScalarType::Boolean,
        'b' => ScalarType::Byte,
        'B' => ScalarType::UByte,
        'h' => ScalarType::Short,
        'H' => ScalarType::UShort,
        'i' => ScalarType::Int,
        'I' => ScalarType::UInt,
        'l' => ScalarType::Long,
        'L' => ScalarType::ULong,
        'f' => ScalarType::Float,
        'd' => ScalarType::Double,
        's' => ScalarType::String,
        _ => return None,
    })
}

fn numpy_dtype(st: ScalarType) -> &'static str {
    match st {
        ScalarType::Boolean => "bool",
        ScalarType::Byte => "int8",
        ScalarType::UByte => "uint8",
        ScalarType::Short => "int16",
        ScalarType::UShort => "uint16",
        ScalarType::Int => "int32",
        ScalarType::UInt => "uint32",
        ScalarType::Long => "int64",
        ScalarType::ULong => "uint64",
        ScalarType::Float => "float32",
        ScalarType::Double => "float64",
        ScalarType::String => "str",
    }
}

/// The p4p type code for a descriptor: a string for scalars, arrays and
/// variants, a tuple `(code, id, members)` for structures and unions.
fn desc_to_py(py: Python<'_>, desc: &FieldDesc) -> PyResult<Py<PyAny>> {
    match desc {
        FieldDesc::Scalar(st) => scalar_code(*st).into_py_any(py),
        FieldDesc::ScalarArray(st) => format!("a{}", scalar_code(*st)).into_py_any(py),
        FieldDesc::Variant => "v".into_py_any(py),
        FieldDesc::VariantArray => "av".into_py_any(py),
        FieldDesc::Structure { struct_id, fields } => members_to_py(py, "S", struct_id, fields),
        FieldDesc::StructureArray { struct_id, fields } => {
            members_to_py(py, "aS", struct_id, fields)
        }
        FieldDesc::Union {
            struct_id,
            variants,
        } => members_to_py(py, "U", struct_id, variants),
        FieldDesc::UnionArray {
            struct_id,
            variants,
        } => members_to_py(py, "aU", struct_id, variants),
    }
}

fn members_to_py(
    py: Python<'_>,
    code: &str,
    id: &str,
    members: &[(String, FieldDesc)],
) -> PyResult<Py<PyAny>> {
    let items = members
        .iter()
        .map(|(n, d)| (n.clone(), desc_to_py(py, d)?).into_py_any(py))
        .collect::<PyResult<Vec<_>>>()?;
    let id: Py<PyAny> = if id.is_empty() {
        py.None()
    } else {
        id.into_py_any(py)?
    };
    (code, id, PyList::new(py, items)?).into_py_any(py)
}

/// Whether `spec` is a `(code, id, members)` compound spec rather than a
/// member list. Distinguished from a 3-member list by the leading string
/// type code (`"S"`, `"aS"`, `"U"`, `"aU"`); a member entry is a `(name,
/// spec)` pair, never a bare string in first position.
fn is_compound_spec(spec: &Bound<'_, PyAny>) -> bool {
    let Ok(t) = spec.cast::<PyTuple>() else {
        return false;
    };
    t.len() == 3
        && t.get_item(0)
            .is_ok_and(|item| item.cast::<PyString>().is_ok())
}

/// Parse one p4p type spec: a code string, a `(code, id, members)` tuple,
/// or a `Type`.
fn desc_from_py(spec: &Bound<'_, PyAny>) -> PyResult<FieldDesc> {
    if let Ok(t) = spec.cast::<Type>() {
        return Ok((*t.get().desc).clone());
    }
    if let Ok(s) = spec.cast::<PyString>() {
        let code = s.to_cow()?;
        let mut chars = code.chars();
        return match (chars.next(), chars.next(), chars.next()) {
            (Some('v'), None, _) => Ok(FieldDesc::Variant),
            (Some('a'), Some('v'), None) => Ok(FieldDesc::VariantArray),
            (Some(c), None, _) => scalar_from_code(c)
                .map(FieldDesc::Scalar)
                .ok_or_else(|| PyValueError::new_err(format!("unknown type code {code:?}"))),
            (Some('a'), Some(c), None) => scalar_from_code(c)
                .map(FieldDesc::ScalarArray)
                .ok_or_else(|| PyValueError::new_err(format!("unknown type code {code:?}"))),
            _ => Err(PyValueError::new_err(format!("unknown type code {code:?}"))),
        };
    }
    let tuple = spec.cast::<PyTuple>().map_err(|_| {
        PyTypeError::new_err(format!(
            "type spec must be a code string, a (code, id, members) tuple or a Type, not {}",
            spec.get_type()
                .name()
                .map(|n| n.to_string())
                .unwrap_or_default()
        ))
    })?;
    if tuple.len() != 3 {
        return Err(PyValueError::new_err(
            "compound type spec must be (code, id, members)",
        ));
    }
    let code: String = tuple.get_item(0)?.extract()?;
    let id_obj = tuple.get_item(1)?;
    let id: String = if id_obj.is_none() {
        String::new()
    } else {
        id_obj.extract()?
    };
    let members = members_from_py(&tuple.get_item(2)?)?;
    match code.as_str() {
        "S" => Ok(FieldDesc::Structure {
            struct_id: id,
            fields: members,
        }),
        "aS" => Ok(FieldDesc::StructureArray {
            struct_id: id,
            fields: members,
        }),
        "U" => Ok(FieldDesc::Union {
            struct_id: id,
            variants: members,
        }),
        "aU" => Ok(FieldDesc::UnionArray {
            struct_id: id,
            variants: members,
        }),
        other => Err(PyValueError::new_err(format!(
            "compound type code must be S, aS, U or aU, not {other:?}"
        ))),
    }
}

fn members_from_py(spec: &Bound<'_, PyAny>) -> PyResult<Vec<(String, FieldDesc)>> {
    if let Ok(t) = spec.cast::<Type>() {
        return match &*t.get().desc {
            FieldDesc::Structure { fields, .. } => Ok(fields.clone()),
            _ => Err(PyTypeError::new_err("member spec Type must be a structure")),
        };
    }
    let mut out = Vec::new();
    for item in spec.try_iter()? {
        let item = item?;
        let pair = item
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("each member must be a (name, spec) tuple"))?;
        if pair.len() != 2 {
            return Err(PyTypeError::new_err(
                "each member must be a (name, spec) tuple",
            ));
        }
        let name: String = pair.get_item(0)?.extract()?;
        out.push((name, desc_from_py(&pair.get_item(1)?)?));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Type
// ---------------------------------------------------------------------------

/// A pvData type description (p4p `Type`).
#[pyclass(name = "Type", module = "epicsrs._epicsrs", frozen)]
pub struct Type {
    pub desc: Arc<FieldDesc>,
}

impl Type {
    pub fn from_desc(desc: Arc<FieldDesc>) -> Self {
        Type { desc }
    }

    fn fields(&self) -> PyResult<&[(String, FieldDesc)]> {
        match &*self.desc {
            FieldDesc::Structure { fields, .. } => Ok(fields),
            _ => Err(PyTypeError::new_err("not a structure type")),
        }
    }
}

#[pymethods]
impl Type {
    /// `Type(spec, id=None)`: `spec` is a list of `(name, code)` members,
    /// or a `(code, id, members)` tuple as returned by [`Type::aspy`] (so
    /// `Type(t.aspy())` round-trips), or another `Type`.
    #[new]
    #[pyo3(signature = (spec, id=None))]
    fn new(spec: &Bound<'_, PyAny>, id: Option<String>) -> PyResult<Self> {
        let desc = if spec.cast::<Type>().is_ok() || is_compound_spec(spec) {
            desc_from_py(spec)?
        } else {
            FieldDesc::Structure {
                struct_id: id.clone().unwrap_or_default(),
                fields: members_from_py(spec)?,
            }
        };
        let desc = match (desc, id) {
            (FieldDesc::Structure { fields, .. }, Some(id)) => FieldDesc::Structure {
                struct_id: id,
                fields,
            },
            (d, _) => d,
        };
        Ok(Type {
            desc: Arc::new(desc),
        })
    }

    #[allow(non_snake_case)]
    fn getID(&self) -> String {
        match &*self.desc {
            FieldDesc::Structure { struct_id, .. }
            | FieldDesc::StructureArray { struct_id, .. }
            | FieldDesc::Union { struct_id, .. }
            | FieldDesc::UnionArray { struct_id, .. } => struct_id.clone(),
            _ => String::new(),
        }
    }

    fn keys(&self) -> PyResult<Vec<String>> {
        Ok(self.fields()?.iter().map(|(n, _)| n.clone()).collect())
    }

    fn items(&self, py: Python<'_>) -> PyResult<Vec<(String, Py<PyAny>)>> {
        self.fields()?
            .iter()
            .map(|(n, d)| Ok((n.clone(), member_type_to_py(py, d)?)))
            .collect()
    }

    fn has(&self, key: &str) -> bool {
        desc_at(&self.desc, key).is_some()
    }

    fn __contains__(&self, key: &str) -> bool {
        self.has(key)
    }

    fn __len__(&self) -> usize {
        self.desc.field_count()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        PyList::new(py, self.keys()?)?
            .as_any()
            .try_iter()?
            .unbind()
            .into_py_any(py)
    }

    /// The member's `Type` for structures, its type code otherwise.
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match desc_at(&self.desc, key) {
            Some(d) => member_type_to_py(py, d),
            None => Err(PyKeyError::new_err(format!("no such member field {key:?}"))),
        }
    }

    /// The spec this type was (or could have been) built from.
    fn aspy(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        desc_to_py(py, &self.desc)
    }

    /// Build a `Value` of this type, optionally assigning `initial`.
    #[pyo3(signature = (initial=None))]
    fn __call__(&self, py: Python<'_>, initial: Option<&Bound<'_, PyAny>>) -> PyResult<Value> {
        Value::create(py, self.desc.clone(), initial)
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.cast::<Type>() {
            Ok(o) => o.get().desc == self.desc,
            Err(_) => false,
        }
    }

    fn __repr__(&self) -> String {
        format!("Type({})", self.desc)
    }
}

fn member_type_to_py(py: Python<'_>, d: &FieldDesc) -> PyResult<Py<PyAny>> {
    match d {
        FieldDesc::Structure { .. } => Type::from_desc(Arc::new(d.clone())).into_py_any(py),
        _ => desc_to_py(py, d),
    }
}

// ---------------------------------------------------------------------------
// Root and Value
// ---------------------------------------------------------------------------

pub struct Root {
    pub desc: Arc<FieldDesc>,
    pub field: PvField,
    pub marks: BitSet,
}

impl Root {
    fn mark_span(&mut self, path: &str, val: bool) -> bool {
        let Some(sub) = desc_at(&self.desc, path) else {
            return false;
        };
        let Some(start) = self.desc.bit_for_path(path) else {
            return false;
        };
        let end = start + sub.total_bits();
        if val {
            for b in start..end {
                self.marks.set(b);
            }
        } else {
            let mut cleared = BitSet::new();
            for b in self.marks.iter().filter(|b| *b < start || *b >= end) {
                cleared.set(b);
            }
            self.marks = cleared;
        }
        true
    }
}

/// A pvData structure value (p4p `Value`).
#[pyclass(name = "Value", module = "epicsrs._epicsrs", frozen)]
pub struct Value {
    root: Arc<Mutex<Root>>,
    prefix: String,
}

impl Value {
    pub fn from_parts(desc: Arc<FieldDesc>, field: PvField, marks: BitSet) -> Self {
        Value {
            root: Arc::new(Mutex::new(Root { desc, field, marks })),
            prefix: String::new(),
        }
    }

    pub fn create(
        py: Python<'_>,
        desc: Arc<FieldDesc>,
        initial: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let field = default_value_for(&desc);
        let v = Value::from_parts(desc, field, BitSet::new());
        if let Some(init) = initial {
            v.assign_root(py, init)?;
        }
        Ok(v)
    }

    fn lock(&self) -> MutexGuard<'_, Root> {
        self.root.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `(desc, field, marks)` of the whole root. Refuses a sub-structure
    /// view, since callers use this to put or post the root.
    pub fn snapshot(&self) -> PyResult<(Arc<FieldDesc>, PvField, BitSet)> {
        self.with_root(|g| (g.desc.clone(), g.field.clone(), g.marks.clone()))
    }

    /// Run `f` on the whole root under its lock, cloning nothing. Refuses a
    /// sub-structure view, as [`Self::snapshot`] does.
    pub fn with_root<R>(&self, f: impl FnOnce(&Root) -> R) -> PyResult<R> {
        if !self.prefix.is_empty() {
            return Err(PyTypeError::new_err(
                "a sub-structure view cannot be sent; pass the root Value",
            ));
        }
        Ok(f(&self.lock()))
    }

    /// Marked leaf paths (structure marks expanded) with their fields.
    pub fn marked_leaves(&self) -> PyResult<Vec<(String, PvField)>> {
        let (desc, field, marks) = self.snapshot()?;
        let mut paths = Vec::new();
        collect_marked(&desc, 0, "", &marks, false, true, &mut paths);
        Ok(paths
            .into_iter()
            .filter_map(|p| field_at(&field, &p).map(|f| (p, f.clone())))
            .collect())
    }

    fn assign_root(&self, py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut g = self.lock();
        let root = &mut *g;
        let desc = root.desc.clone();
        let Some(sub) = desc_at(&desc, &self.prefix) else {
            return Err(PyKeyError::new_err("view path vanished"));
        };
        let bit = desc.bit_for_path(&self.prefix).unwrap_or(0);
        let target = field_at_mut(&mut root.field, &self.prefix)
            .ok_or_else(|| PyKeyError::new_err("view path vanished"))?;
        assign(py, sub, target, obj, &mut root.marks, bit)
    }

    fn full(&self, key: &str) -> String {
        join(&self.prefix, key)
    }

    fn view(&self, path: String) -> Value {
        Value {
            root: self.root.clone(),
            prefix: path,
        }
    }

    fn item(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let path = self.full(key);
        let g = self.lock();
        let Some(desc) = desc_at(&g.desc, &path) else {
            return Err(PyKeyError::new_err(format!("no such member field {key:?}")));
        };
        if matches!(desc, FieldDesc::Structure { .. }) {
            drop(g);
            return self.view(path).into_py_any(py);
        }
        let field = field_at(&g.field, &path)
            .ok_or_else(|| PyKeyError::new_err(format!("no such member field {key:?}")))?;
        field_to_py(py, desc, field)
    }
}

/// Collect marked paths under `desc` (own bit `bit`, path `prefix`).
/// `inherited` means an ancestor is marked. With `expand`, marked
/// structures contribute their leaves instead of themselves.
fn collect_marked(
    desc: &FieldDesc,
    bit: usize,
    prefix: &str,
    marks: &BitSet,
    inherited: bool,
    expand: bool,
    out: &mut Vec<String>,
) {
    let marked = inherited || marks.get(bit);
    let is_struct = matches!(desc, FieldDesc::Structure { .. });
    if marked && !(expand && is_struct) {
        if !prefix.is_empty() {
            out.push(prefix.to_string());
        }
        if !expand {
            return;
        }
    }
    if is_struct {
        for (name, child, cbit) in children(desc, bit) {
            let path = join(prefix, name);
            collect_marked(child, cbit, &path, marks, marked && expand, expand, out);
        }
    }
}

#[pymethods]
impl Value {
    /// `Value(type, initial=None)`.
    #[new]
    #[pyo3(signature = (r#type, initial=None))]
    fn new(
        py: Python<'_>,
        r#type: &Bound<'_, PyAny>,
        initial: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let desc = match r#type.cast::<Type>() {
            Ok(t) => t.get().desc.clone(),
            Err(_) => Arc::new(desc_from_py(r#type)?),
        };
        Value::create(py, desc, initial)
    }

    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        self.item(py, key)
    }

    fn __setitem__(&self, py: Python<'_>, key: &str, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let path = self.full(key);
        let mut g = self.lock();
        let root = &mut *g;
        let desc = root.desc.clone();
        let Some(sub) = desc_at(&desc, &path) else {
            return Err(PyKeyError::new_err(format!("no such member field {key:?}")));
        };
        let bit = desc.bit_for_path(&path).unwrap_or(0);
        let target = field_at_mut(&mut root.field, &path)
            .ok_or_else(|| PyKeyError::new_err(format!("no such member field {key:?}")))?;
        assign(py, sub, target, val, &mut root.marks, bit)
    }

    fn __getattr__(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        if name.starts_with('_') {
            return Err(PyAttributeError::new_err(name.to_string()));
        }
        self.item(py, name)
            .map_err(|_| PyAttributeError::new_err(format!("no such member field {name:?}")))
    }

    fn __setattr__(&self, py: Python<'_>, name: &str, val: &Bound<'_, PyAny>) -> PyResult<()> {
        if name.starts_with('_') {
            return Err(PyAttributeError::new_err(name.to_string()));
        }
        self.__setitem__(py, name, val)
    }

    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        match self.item(py, key) {
            Ok(v) => Ok(v),
            Err(e) if e.is_instance_of::<PyKeyError>(py) => {
                Ok(default.unwrap_or_else(|| py.None()))
            }
            Err(e) => Err(e),
        }
    }

    fn has(&self, key: &str) -> bool {
        desc_at(&self.lock().desc, &self.full(key)).is_some()
    }

    fn __contains__(&self, key: &str) -> bool {
        self.has(key)
    }

    fn keys(&self) -> PyResult<Vec<String>> {
        let g = self.lock();
        match desc_at(&g.desc, &self.prefix) {
            Some(FieldDesc::Structure { fields, .. }) => {
                Ok(fields.iter().map(|(n, _)| n.clone()).collect())
            }
            _ => Err(PyTypeError::new_err("not a structure")),
        }
    }

    fn items(&self, py: Python<'_>) -> PyResult<Vec<(String, Py<PyAny>)>> {
        self.keys()?
            .into_iter()
            .map(|k| Ok((k.clone(), self.item(py, &k)?)))
            .collect()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        PyList::new(py, self.keys()?)?
            .as_any()
            .try_iter()?
            .unbind()
            .into_py_any(py)
    }

    fn __len__(&self) -> usize {
        desc_at(&self.lock().desc, &self.prefix).map_or(0, |d| d.field_count())
    }

    #[allow(non_snake_case)]
    fn getID(&self) -> PyResult<String> {
        let g = self.lock();
        match desc_at(&g.desc, &self.prefix) {
            Some(FieldDesc::Structure { struct_id, .. }) => Ok(struct_id.clone()),
            _ => Err(PyTypeError::new_err("not a structure")),
        }
    }

    /// The `Type` of this structure, or of member `field`.
    #[pyo3(signature = (field=None))]
    fn r#type(&self, py: Python<'_>, field: Option<&str>) -> PyResult<Py<PyAny>> {
        let path = self.full(field.unwrap_or(""));
        let g = self.lock();
        match desc_at(&g.desc, &path) {
            Some(d) => member_type_to_py(py, d),
            None => Err(PyKeyError::new_err(format!(
                "no such member field {:?}",
                field.unwrap_or("")
            ))),
        }
    }

    /// True if any of `fields` (or, with none given, anything) is marked
    /// changed. A field counts as changed when it, an ancestor, or a
    /// descendant is marked.
    #[pyo3(signature = (*fields))]
    fn changed(&self, fields: &Bound<'_, PyTuple>) -> PyResult<bool> {
        let g = self.lock();
        let names: Vec<String> = if fields.is_empty() {
            vec![String::new()]
        } else {
            fields.extract()?
        };
        for name in names {
            let path = self.full(&name);
            let Some(sub) = desc_at(&g.desc, &path) else {
                return Err(PyKeyError::new_err(format!(
                    "no such member field {name:?}"
                )));
            };
            let start = g.desc.bit_for_path(&path).unwrap_or(0);
            let end = start + sub.total_bits();
            if (start..end).any(|b| g.marks.get(b)) {
                return Ok(true);
            }
            // Ancestors, innermost first.
            let mut p = path.as_str();
            while let Some(i) = p.rfind('.') {
                p = &p[..i];
                if g.desc.bit_for_path(p).is_some_and(|b| g.marks.get(b)) {
                    return Ok(true);
                }
            }
            if g.marks.get(0) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The set of marked field paths, relative to this view.
    #[allow(non_snake_case)]
    #[pyo3(signature = (expand=false, parents=false))]
    fn changedSet(&self, py: Python<'_>, expand: bool, parents: bool) -> PyResult<Py<PySet>> {
        let g = self.lock();
        let Some(sub) = desc_at(&g.desc, &self.prefix) else {
            return Err(PyKeyError::new_err("view path vanished"));
        };
        let bit = g.desc.bit_for_path(&self.prefix).unwrap_or(0);
        let mut ancestor = g.marks.get(0);
        let mut p = self.prefix.as_str();
        while let Some(i) = p.rfind('.') {
            p = &p[..i];
            ancestor |= g.desc.bit_for_path(p).is_some_and(|b| g.marks.get(b));
        }
        let mut paths = Vec::new();
        collect_marked(sub, bit, "", &g.marks, ancestor, expand, &mut paths);
        let mut out: BTreeSet<String> = paths.into_iter().collect();
        if parents {
            for path in out.clone() {
                let mut p = path.as_str();
                while let Some(i) = p.rfind('.') {
                    p = &p[..i];
                    out.insert(p.to_string());
                }
            }
        }
        let set = PySet::empty(py)?;
        for p in out {
            set.add(p)?;
        }
        Ok(set.unbind())
    }

    /// Mark `field` (or, with none, every field of this view) as changed.
    #[pyo3(signature = (field=None, val=true))]
    fn mark(&self, field: Option<&str>, val: bool) -> PyResult<()> {
        let path = self.full(field.unwrap_or(""));
        let mut g = self.lock();
        if field.is_some() {
            let Some(bit) = g.desc.bit_for_path(&path) else {
                return Err(PyKeyError::new_err(format!(
                    "no such member field {:?}",
                    field.unwrap_or("")
                )));
            };
            if val {
                g.marks.set(bit);
            } else {
                g.mark_span(&path, false);
            }
            return Ok(());
        }
        if !g.mark_span(&path, val) {
            return Err(PyKeyError::new_err("view path vanished"));
        }
        Ok(())
    }

    #[pyo3(signature = (field=None))]
    fn unmark(&self, field: Option<&str>) -> PyResult<()> {
        self.mark(field, false)
    }

    /// Select union member `selector` (a name, or `None` to unselect) of
    /// union field `field`.
    #[pyo3(signature = (field, selector=None))]
    fn select(&self, field: &str, selector: Option<&str>) -> PyResult<()> {
        let path = self.full(field);
        let mut g = self.lock();
        let root = &mut *g;
        let desc = root.desc.clone();
        let Some(FieldDesc::Union { variants, .. }) = desc_at(&desc, &path) else {
            return Err(PyKeyError::new_err(format!(
                "no such union field {field:?}"
            )));
        };
        let target = field_at_mut(&mut root.field, &path)
            .ok_or_else(|| PyKeyError::new_err(format!("no such union field {field:?}")))?;
        *target = match selector {
            None => PvField::Union {
                selector: -1,
                variant_name: String::new(),
                value: Box::new(PvField::Null),
            },
            Some(name) => {
                let (idx, (vname, vdesc)) = variants
                    .iter()
                    .enumerate()
                    .find(|(_, (n, _))| n == name)
                    .ok_or_else(|| PyKeyError::new_err(format!("union has no member {name:?}")))?;
                PvField::Union {
                    selector: idx as i32,
                    variant_name: vname.clone(),
                    value: Box::new(default_value_for(vdesc)),
                }
            }
        };
        if let Some(bit) = desc.bit_for_path(&path) {
            root.marks.set(bit);
        }
        Ok(())
    }

    /// A dict of this structure; nested structures become dicts.
    #[pyo3(signature = (fields=None))]
    fn todict(&self, py: Python<'_>, fields: Option<Vec<String>>) -> PyResult<Py<PyAny>> {
        let g = self.lock();
        let desc = desc_at(&g.desc, &self.prefix)
            .ok_or_else(|| PyKeyError::new_err("view path vanished"))?;
        let field = field_at(&g.field, &self.prefix)
            .ok_or_else(|| PyKeyError::new_err("view path vanished"))?;
        let FieldDesc::Structure {
            fields: members, ..
        } = desc
        else {
            return Err(PyTypeError::new_err("not a structure"));
        };
        let PvField::Structure(s) = field else {
            return Err(PyTypeError::new_err("not a structure"));
        };
        let out = PyDict::new(py);
        for (name, child) in members {
            if fields
                .as_ref()
                .is_some_and(|f| !f.iter().any(|x| x == name))
            {
                continue;
            }
            let Some(v) = s.get_field(name) else { continue };
            out.set_item(name, field_to_plain_py(py, child, v)?)?;
        }
        Ok(out.into_any().unbind())
    }

    /// A list of `(name, value)` pairs; nested structures become lists.
    fn tolist(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let g = self.lock();
        let desc = desc_at(&g.desc, &self.prefix)
            .ok_or_else(|| PyKeyError::new_err("view path vanished"))?;
        let field = field_at(&g.field, &self.prefix)
            .ok_or_else(|| PyKeyError::new_err("view path vanished"))?;
        struct_to_list(py, desc, field)
    }

    fn tostr(&self) -> PyResult<String> {
        let g = self.lock();
        let field = field_at(&g.field, &self.prefix)
            .ok_or_else(|| PyKeyError::new_err("view path vanished"))?;
        Ok(render_value(field))
    }

    fn __repr__(&self) -> PyResult<String> {
        let g = self.lock();
        let desc = desc_at(&g.desc, &self.prefix)
            .ok_or_else(|| PyKeyError::new_err("view path vanished"))?;
        let id = match desc {
            FieldDesc::Structure { struct_id, .. } => struct_id.as_str(),
            _ => "",
        };
        let field = field_at(&g.field, &self.prefix)
            .ok_or_else(|| PyKeyError::new_err("view path vanished"))?;
        Ok(format!("Value({id:?}, {})", render_value(field).trim_end()))
    }

    fn __str__(&self) -> PyResult<String> {
        self.tostr()
    }
}

fn struct_to_list(py: Python<'_>, desc: &FieldDesc, field: &PvField) -> PyResult<Py<PyAny>> {
    let (
        FieldDesc::Structure {
            fields: members, ..
        },
        PvField::Structure(s),
    ) = (desc, field)
    else {
        return Err(PyTypeError::new_err("not a structure"));
    };
    let mut items = Vec::with_capacity(members.len());
    for (name, child) in members {
        let Some(v) = s.get_field(name) else { continue };
        let conv = if matches!(child, FieldDesc::Structure { .. }) {
            struct_to_list(py, child, v)?
        } else {
            field_to_plain_py(py, child, v)?
        };
        items.push((name.clone(), conv).into_py_any(py)?);
    }
    Ok(PyList::new(py, items)?.into_any().unbind())
}

// ---------------------------------------------------------------------------
// PvField -> Python
// ---------------------------------------------------------------------------

pub fn scalar_to_py(py: Python<'_>, v: &ScalarValue) -> PyResult<Py<PyAny>> {
    match v {
        ScalarValue::Boolean(b) => b.into_py_any(py),
        ScalarValue::Byte(x) => x.into_py_any(py),
        ScalarValue::UByte(x) => x.into_py_any(py),
        ScalarValue::Short(x) => x.into_py_any(py),
        ScalarValue::UShort(x) => x.into_py_any(py),
        ScalarValue::Int(x) => x.into_py_any(py),
        ScalarValue::UInt(x) => x.into_py_any(py),
        ScalarValue::Long(x) => x.into_py_any(py),
        ScalarValue::ULong(x) => x.into_py_any(py),
        ScalarValue::Float(x) => x.into_py_any(py),
        ScalarValue::Double(x) => x.into_py_any(py),
        ScalarValue::String(s) => s.as_str_lossy().into_py_any(py),
    }
}

/// A read-only numpy array over `data`, whose base object keeps the
/// `Arc` alive. No element is copied.
fn arc_to_numpy<T: Element + Copy + Send + Sync + 'static>(
    py: Python<'_>,
    data: &Arc<[T]>,
) -> PyResult<Py<PyAny>> {
    let keep = data.clone();
    let capsule = PyCapsule::new(py, keep, Some(CString::new("epicsrs.pva.array").unwrap()))?;
    let view = ArrayView1::from(&data[..]);
    // SAFETY: the capsule owns a clone of the Arc, so the buffer outlives
    // the numpy array; an `Arc<[T]>` is never reallocated.
    let arr = unsafe { PyArray1::<T>::borrow_from_array(&view, capsule.into_any()) };
    let ro = arr.readwrite().make_nonwriteable();
    let out: Bound<'_, PyAny> = (*ro).clone().into_any();
    Ok(out.unbind())
}

pub fn typed_array_to_py(py: Python<'_>, arr: &TypedScalarArray) -> PyResult<Py<PyAny>> {
    match arr {
        TypedScalarArray::Boolean(a) => arc_to_numpy(py, a),
        TypedScalarArray::Byte(a) => arc_to_numpy(py, a),
        TypedScalarArray::UByte(a) => arc_to_numpy(py, a),
        TypedScalarArray::Short(a) => arc_to_numpy(py, a),
        TypedScalarArray::UShort(a) => arc_to_numpy(py, a),
        TypedScalarArray::Int(a) => arc_to_numpy(py, a),
        TypedScalarArray::UInt(a) => arc_to_numpy(py, a),
        TypedScalarArray::Long(a) => arc_to_numpy(py, a),
        TypedScalarArray::ULong(a) => arc_to_numpy(py, a),
        TypedScalarArray::Float(a) => arc_to_numpy(py, a),
        TypedScalarArray::Double(a) => arc_to_numpy(py, a),
        TypedScalarArray::String(a) => {
            let items = a
                .iter()
                .map(|s| s.as_str_lossy().into_py_any(py))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, items)?.into_any().unbind())
        }
    }
}

fn untyped_array_to_py(py: Python<'_>, st: ScalarType, v: &[ScalarValue]) -> PyResult<Py<PyAny>> {
    match TypedScalarArray::from_scalar_values(v, st) {
        Some(t) => typed_array_to_py(py, &t),
        None => {
            let items = v
                .iter()
                .map(|s| scalar_to_py(py, s))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, items)?.into_any().unbind())
        }
    }
}

fn detached(desc: &FieldDesc, field: &PvField) -> Value {
    Value::from_parts(Arc::new(desc.clone()), field.clone(), BitSet::new())
}

/// Convert a non-structure field; structures become detached `Value`s.
pub fn field_to_py(py: Python<'_>, desc: &FieldDesc, field: &PvField) -> PyResult<Py<PyAny>> {
    match (desc, field) {
        (_, PvField::Scalar(v)) => scalar_to_py(py, v),
        (_, PvField::ScalarArrayTyped(a)) => typed_array_to_py(py, a),
        (FieldDesc::ScalarArray(st), PvField::ScalarArray(v)) => untyped_array_to_py(py, *st, v),
        (_, PvField::ScalarArray(v)) => {
            let st = v.first().map_or(ScalarType::Double, |s| s.scalar_type());
            untyped_array_to_py(py, st, v)
        }
        (FieldDesc::Structure { .. }, PvField::Structure(_)) => {
            detached(desc, field).into_py_any(py)
        }
        (FieldDesc::StructureArray { struct_id, fields }, PvField::StructureArray(items)) => {
            let elem = FieldDesc::Structure {
                struct_id: struct_id.clone(),
                fields: fields.clone(),
            };
            let out = items
                .iter()
                .map(|it| match it {
                    Some(s) => detached(&elem, &PvField::Structure(s.clone())).into_py_any(py),
                    None => Ok(py.None()),
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, out)?.into_any().unbind())
        }
        (
            FieldDesc::Union { variants, .. },
            PvField::Union {
                selector, value, ..
            },
        ) => match variants.get(usize::try_from(*selector).unwrap_or(usize::MAX)) {
            Some((_, vdesc)) => field_to_py(py, vdesc, value),
            None => Ok(py.None()),
        },
        (FieldDesc::UnionArray { variants, .. }, PvField::UnionArray(items)) => {
            let out = items
                .iter()
                .map(|it| match it {
                    Some(UnionItem {
                        selector, value, ..
                    }) => match variants.get(usize::try_from(*selector).unwrap_or(usize::MAX)) {
                        Some((_, vdesc)) => field_to_py(py, vdesc, value),
                        None => Ok(py.None()),
                    },
                    None => Ok(py.None()),
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, out)?.into_any().unbind())
        }
        (_, PvField::Variant(v)) => variant_to_py(py, v),
        (_, PvField::VariantArray(items)) => {
            let out = items
                .iter()
                .map(|it| match it {
                    Some(v) => variant_to_py(py, v),
                    None => Ok(py.None()),
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, out)?.into_any().unbind())
        }
        (_, PvField::Null) => Ok(py.None()),
        (d, f) => Err(PyTypeError::new_err(format!(
            "value {f} does not fit descriptor {d}"
        ))),
    }
}

fn variant_to_py(py: Python<'_>, v: &VariantValue) -> PyResult<Py<PyAny>> {
    match (&v.desc, &v.value) {
        (_, PvField::Null) => Ok(py.None()),
        (Some(d), f) => field_to_py(py, d, f),
        (None, f) => field_to_py(py, &f.descriptor(), f),
    }
}

/// Like `field_to_py` but structures become dicts (for `todict`).
fn field_to_plain_py(py: Python<'_>, desc: &FieldDesc, field: &PvField) -> PyResult<Py<PyAny>> {
    match (desc, field) {
        (
            FieldDesc::Structure {
                fields: members, ..
            },
            PvField::Structure(s),
        ) => {
            let out = PyDict::new(py);
            for (name, child) in members {
                if let Some(v) = s.get_field(name) {
                    out.set_item(name, field_to_plain_py(py, child, v)?)?;
                }
            }
            Ok(out.into_any().unbind())
        }
        (FieldDesc::StructureArray { struct_id, fields }, PvField::StructureArray(items)) => {
            let elem = FieldDesc::Structure {
                struct_id: struct_id.clone(),
                fields: fields.clone(),
            };
            let out = items
                .iter()
                .map(|it| match it {
                    Some(s) => field_to_plain_py(py, &elem, &PvField::Structure(s.clone())),
                    None => Ok(py.None()),
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, out)?.into_any().unbind())
        }
        (
            FieldDesc::Union { variants, .. },
            PvField::Union {
                selector, value, ..
            },
        ) => match variants.get(usize::try_from(*selector).unwrap_or(usize::MAX)) {
            Some((_, vdesc)) => field_to_plain_py(py, vdesc, value),
            None => Ok(py.None()),
        },
        (_, PvField::Variant(v)) => match (&v.desc, &v.value) {
            (_, PvField::Null) => Ok(py.None()),
            (Some(d), f) => field_to_plain_py(py, d, f),
            (None, f) => field_to_plain_py(py, &f.descriptor(), f),
        },
        _ => field_to_py(py, desc, field),
    }
}

// ---------------------------------------------------------------------------
// Python -> PvField
// ---------------------------------------------------------------------------

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_default()
}

pub fn py_to_scalar(obj: &Bound<'_, PyAny>, st: ScalarType) -> PyResult<ScalarValue> {
    if let Ok(s) = obj.cast::<PyString>() {
        let s = s.to_cow()?;
        return ScalarValue::parse(st, &s).map_err(PyValueError::new_err);
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        let b = b.is_true();
        return Ok(match st {
            ScalarType::Boolean => ScalarValue::Boolean(b),
            ScalarType::String => ScalarValue::String(if b { "true" } else { "false" }.into()),
            _ => int_to_scalar(b as i128, st),
        });
    }
    if st == ScalarType::String {
        return Ok(ScalarValue::String(obj.str()?.to_cow()?.as_ref().into()));
    }
    if obj.cast::<PyInt>().is_ok() {
        let i: i128 = obj.extract()?;
        return Ok(match st {
            ScalarType::Boolean => ScalarValue::Boolean(i != 0),
            ScalarType::Float => ScalarValue::Float(i as f32),
            ScalarType::Double => ScalarValue::Double(i as f64),
            _ => int_to_scalar(i, st),
        });
    }
    if obj.cast::<PyFloat>().is_ok() {
        let f: f64 = obj.extract()?;
        return Ok(float_to_scalar(f, st));
    }
    // numpy scalars and anything else with __index__ / __float__.
    if let Ok(i) = obj.extract::<i128>() {
        return Ok(match st {
            ScalarType::Boolean => ScalarValue::Boolean(i != 0),
            ScalarType::Float => ScalarValue::Float(i as f32),
            ScalarType::Double => ScalarValue::Double(i as f64),
            _ => int_to_scalar(i, st),
        });
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(float_to_scalar(f, st));
    }
    Err(PyTypeError::new_err(format!(
        "cannot assign {} to a {st:?} field",
        type_name(obj)
    )))
}

fn int_to_scalar(i: i128, st: ScalarType) -> ScalarValue {
    match st {
        ScalarType::Boolean => ScalarValue::Boolean(i != 0),
        ScalarType::Byte => ScalarValue::Byte(i as i8),
        ScalarType::UByte => ScalarValue::UByte(i as u8),
        ScalarType::Short => ScalarValue::Short(i as i16),
        ScalarType::UShort => ScalarValue::UShort(i as u16),
        ScalarType::Int => ScalarValue::Int(i as i32),
        ScalarType::UInt => ScalarValue::UInt(i as u32),
        ScalarType::Long => ScalarValue::Long(i as i64),
        ScalarType::ULong => ScalarValue::ULong(i as u64),
        ScalarType::Float => ScalarValue::Float(i as f32),
        ScalarType::Double => ScalarValue::Double(i as f64),
        ScalarType::String => ScalarValue::String(i.to_string().into()),
    }
}

fn float_to_scalar(f: f64, st: ScalarType) -> ScalarValue {
    match st {
        ScalarType::Float => ScalarValue::Float(f as f32),
        ScalarType::Double => ScalarValue::Double(f),
        ScalarType::String => ScalarValue::String(f.to_string().into()),
        _ => int_to_scalar(f as i128, st),
    }
}

fn numpy_1d<'py, T: Element>(
    py: Python<'py>,
    obj: &Bound<'py, PyAny>,
    dtype: &str,
) -> PyResult<PyReadonlyArray1<'py, T>> {
    let np = py.import("numpy")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("dtype", dtype)?;
    let arr = np.getattr("asarray")?.call((obj,), Some(&kwargs))?;
    let arr = if arr.getattr("ndim")?.extract::<usize>()? != 1 {
        arr.call_method0("ravel")?
    } else {
        arr
    };
    Ok(arr.extract::<PyReadonlyArray1<T>>()?)
}

fn to_arc<T: Element + Copy>(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    dtype: &str,
) -> PyResult<Arc<[T]>> {
    let a = numpy_1d::<T>(py, obj, dtype)?;
    Ok(Arc::from(a.as_slice()?.to_vec().into_boxed_slice()))
}

pub fn py_to_array(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    st: ScalarType,
) -> PyResult<TypedScalarArray> {
    let d = numpy_dtype(st);
    Ok(match st {
        ScalarType::Boolean => TypedScalarArray::Boolean(to_arc(py, obj, d)?),
        ScalarType::Byte => TypedScalarArray::Byte(to_arc(py, obj, d)?),
        ScalarType::UByte => TypedScalarArray::UByte(to_arc(py, obj, d)?),
        ScalarType::Short => TypedScalarArray::Short(to_arc(py, obj, d)?),
        ScalarType::UShort => TypedScalarArray::UShort(to_arc(py, obj, d)?),
        ScalarType::Int => TypedScalarArray::Int(to_arc(py, obj, d)?),
        ScalarType::UInt => TypedScalarArray::UInt(to_arc(py, obj, d)?),
        ScalarType::Long => TypedScalarArray::Long(to_arc(py, obj, d)?),
        ScalarType::ULong => TypedScalarArray::ULong(to_arc(py, obj, d)?),
        ScalarType::Float => TypedScalarArray::Float(to_arc(py, obj, d)?),
        ScalarType::Double => TypedScalarArray::Double(to_arc(py, obj, d)?),
        ScalarType::String => {
            let items = if obj.cast::<PyString>().is_ok() {
                vec![obj.str()?.to_cow()?.as_ref().into()]
            } else {
                obj.try_iter()?
                    .map(|it| Ok(it?.str()?.to_cow()?.as_ref().into()))
                    .collect::<PyResult<Vec<_>>>()?
            };
            TypedScalarArray::String(Arc::from(items.into_boxed_slice()))
        }
    })
}

/// The descriptor a Python object implies when assigned to a variant.
fn infer_desc(obj: &Bound<'_, PyAny>) -> PyResult<Option<FieldDesc>> {
    if obj.is_none() {
        return Ok(None);
    }
    if let Ok(v) = obj.cast::<Value>() {
        let v = v.get();
        let g = v.lock();
        return Ok(desc_at(&g.desc, &v.prefix).cloned());
    }
    if obj.cast::<PyBool>().is_ok() {
        return Ok(Some(FieldDesc::Scalar(ScalarType::Boolean)));
    }
    if obj.cast::<PyInt>().is_ok() {
        return Ok(Some(FieldDesc::Scalar(ScalarType::Long)));
    }
    if obj.cast::<PyFloat>().is_ok() {
        return Ok(Some(FieldDesc::Scalar(ScalarType::Double)));
    }
    if obj.cast::<PyString>().is_ok() {
        return Ok(Some(FieldDesc::Scalar(ScalarType::String)));
    }
    if obj.hasattr("dtype")? && obj.hasattr("shape")? {
        let kind: String = obj.getattr("dtype")?.getattr("kind")?.extract()?;
        let size: usize = obj.getattr("dtype")?.getattr("itemsize")?.extract()?;
        let st = match (kind.as_str(), size) {
            ("b", _) => ScalarType::Boolean,
            ("i", 1) => ScalarType::Byte,
            ("i", 2) => ScalarType::Short,
            ("i", 4) => ScalarType::Int,
            ("i", _) => ScalarType::Long,
            ("u", 1) => ScalarType::UByte,
            ("u", 2) => ScalarType::UShort,
            ("u", 4) => ScalarType::UInt,
            ("u", _) => ScalarType::ULong,
            ("f", 4) => ScalarType::Float,
            ("f", _) => ScalarType::Double,
            _ => ScalarType::String,
        };
        let ndim: usize = obj.getattr("ndim")?.extract()?;
        return Ok(Some(if ndim == 0 {
            FieldDesc::Scalar(st)
        } else {
            FieldDesc::ScalarArray(st)
        }));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        let all_str = list.len() > 0 && list.iter().all(|e| e.is_instance_of::<PyString>());
        return Ok(Some(FieldDesc::ScalarArray(if all_str {
            ScalarType::String
        } else {
            ScalarType::Double
        })));
    }
    if let Ok(i) = obj.extract::<i128>() {
        let _ = i;
        return Ok(Some(FieldDesc::Scalar(ScalarType::Long)));
    }
    if obj.extract::<f64>().is_ok() {
        return Ok(Some(FieldDesc::Scalar(ScalarType::Double)));
    }
    Err(PyTypeError::new_err(format!(
        "cannot infer a pvData type for {}",
        type_name(obj)
    )))
}

/// Build a fresh field of `desc` from `obj`.
fn build(py: Python<'_>, desc: &FieldDesc, obj: &Bound<'_, PyAny>) -> PyResult<PvField> {
    let mut f = default_value_for(desc);
    let mut scratch = BitSet::new();
    assign(py, desc, &mut f, obj, &mut scratch, 0)?;
    Ok(f)
}

fn pick_union_variant<'a>(
    variants: &'a [(String, FieldDesc)],
    obj: &Bound<'_, PyAny>,
) -> PyResult<(usize, &'a str, &'a FieldDesc)> {
    let wanted = infer_desc(obj)?;
    let pred = |d: &FieldDesc| -> bool {
        match (&wanted, d) {
            (
                Some(FieldDesc::Scalar(ScalarType::String)),
                FieldDesc::Scalar(ScalarType::String),
            ) => true,
            (
                Some(FieldDesc::Scalar(ScalarType::Boolean)),
                FieldDesc::Scalar(ScalarType::Boolean),
            ) => true,
            (Some(FieldDesc::Scalar(ScalarType::Long)), FieldDesc::Scalar(st)) => {
                !matches!(st, ScalarType::String | ScalarType::Boolean)
            }
            (Some(FieldDesc::Scalar(ScalarType::Double)), FieldDesc::Scalar(st)) => {
                matches!(st, ScalarType::Double | ScalarType::Float)
            }
            (Some(FieldDesc::ScalarArray(w)), FieldDesc::ScalarArray(st)) => w == st,
            (Some(FieldDesc::Structure { .. }), FieldDesc::Structure { .. }) => true,
            _ => false,
        }
    };
    if let Some((i, (n, d))) = variants.iter().enumerate().find(|(_, (_, d))| pred(d)) {
        return Ok((i, n, d));
    }
    // Second pass: any array member for an array, any numeric for a number.
    let loose = |d: &FieldDesc| -> bool {
        match (&wanted, d) {
            (Some(FieldDesc::ScalarArray(_)), FieldDesc::ScalarArray(_)) => true,
            (Some(FieldDesc::Scalar(ScalarType::Double)), FieldDesc::Scalar(st)) => {
                !matches!(st, ScalarType::String | ScalarType::Boolean)
            }
            _ => false,
        }
    };
    if let Some((i, (n, d))) = variants.iter().enumerate().find(|(_, (_, d))| loose(d)) {
        return Ok((i, n, d));
    }
    if obj.cast::<PyDict>().is_ok() {
        if let Some((i, (n, d))) = variants
            .iter()
            .enumerate()
            .find(|(_, (_, d))| matches!(d, FieldDesc::Structure { .. }))
        {
            return Ok((i, n, d));
        }
    }
    Err(PyValueError::new_err(format!(
        "no union member accepts a {}; select one with (name, value)",
        type_name(obj)
    )))
}

fn build_union(
    py: Python<'_>,
    variants: &[(String, FieldDesc)],
    obj: &Bound<'_, PyAny>,
) -> PyResult<PvField> {
    if obj.is_none() {
        return Ok(PvField::Union {
            selector: -1,
            variant_name: String::new(),
            value: Box::new(PvField::Null),
        });
    }
    let (idx, name, vdesc, payload) = if let Ok(t) = obj.cast::<PyTuple>() {
        if t.len() != 2 {
            return Err(PyValueError::new_err(
                "union assignment tuple must be (member, value)",
            ));
        }
        let name: String = t.get_item(0)?.extract()?;
        let (idx, (n, d)) = variants
            .iter()
            .enumerate()
            .find(|(_, (n, _))| *n == name)
            .ok_or_else(|| PyKeyError::new_err(format!("union has no member {name:?}")))?;
        (idx, n.as_str(), d, t.get_item(1)?)
    } else {
        let (idx, n, d) = pick_union_variant(variants, obj)?;
        (idx, n, d, obj.clone())
    };
    Ok(PvField::Union {
        selector: idx as i32,
        variant_name: name.to_string(),
        value: Box::new(build(py, vdesc, &payload)?),
    })
}

fn build_variant(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<VariantValue> {
    match infer_desc(obj)? {
        None => Ok(VariantValue {
            desc: None,
            value: PvField::Null,
        }),
        Some(desc) => {
            let value = build(py, &desc, obj)?;
            Ok(VariantValue {
                desc: Some(desc),
                value,
            })
        }
    }
}

/// Assign `obj` into `target` (a field of type `desc` whose bit index is
/// `bit`), marking what changed.
pub fn assign(
    py: Python<'_>,
    desc: &FieldDesc,
    target: &mut PvField,
    obj: &Bound<'_, PyAny>,
    marks: &mut BitSet,
    bit: usize,
) -> PyResult<()> {
    match desc {
        FieldDesc::Scalar(st) => {
            *target = PvField::Scalar(py_to_scalar(obj, *st)?);
            marks.set(bit);
        }
        FieldDesc::ScalarArray(st) => {
            *target = PvField::ScalarArrayTyped(py_to_array(py, obj, *st)?);
            marks.set(bit);
        }
        FieldDesc::Structure { fields, .. } => {
            let obj = match obj.cast::<Value>() {
                Ok(v) => v.get().todict(py, None)?.into_bound(py),
                Err(_) => obj.clone(),
            };
            let dict = obj.cast::<PyDict>().map_err(|_| {
                PyTypeError::new_err(format!(
                    "a structure takes a dict or Value, not {}",
                    type_name(&obj)
                ))
            })?;
            let PvField::Structure(s) = target else {
                return Err(PyTypeError::new_err("target is not a structure"));
            };
            let offsets = children(desc, bit)
                .into_iter()
                .map(|(n, _, b)| (n.to_string(), b))
                .collect::<Vec<_>>();
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                let (idx, (_, cdesc)) = fields
                    .iter()
                    .enumerate()
                    .find(|(_, (n, _))| *n == key)
                    .ok_or_else(|| PyKeyError::new_err(format!("no such member field {key:?}")))?;
                let ctarget = s
                    .get_field_mut(&key)
                    .ok_or_else(|| PyKeyError::new_err(format!("no such member field {key:?}")))?;
                assign(py, cdesc, ctarget, &v, marks, offsets[idx].1)?;
            }
        }
        FieldDesc::StructureArray { struct_id, fields } => {
            let elem = FieldDesc::Structure {
                struct_id: struct_id.clone(),
                fields: fields.clone(),
            };
            let mut items = Vec::new();
            for it in obj.try_iter()? {
                let it = it?;
                if it.is_none() {
                    items.push(None);
                    continue;
                }
                match build(py, &elem, &it)? {
                    PvField::Structure(s) => items.push(Some(s)),
                    _ => unreachable!("a structure descriptor builds a structure"),
                }
            }
            *target = PvField::StructureArray(items);
            marks.set(bit);
        }
        FieldDesc::Union { variants, .. } => {
            *target = build_union(py, variants, obj)?;
            marks.set(bit);
        }
        FieldDesc::UnionArray { variants, .. } => {
            let mut items = Vec::new();
            for it in obj.try_iter()? {
                let it = it?;
                match build_union(py, variants, &it)? {
                    PvField::Union { selector: -1, .. } => items.push(None),
                    PvField::Union {
                        selector,
                        variant_name,
                        value,
                    } => items.push(Some(UnionItem {
                        selector,
                        variant_name,
                        value: *value,
                    })),
                    _ => unreachable!("build_union builds a union"),
                }
            }
            *target = PvField::UnionArray(items);
            marks.set(bit);
        }
        FieldDesc::Variant => {
            *target = PvField::Variant(Box::new(build_variant(py, obj)?));
            marks.set(bit);
        }
        FieldDesc::VariantArray => {
            let mut items = Vec::new();
            for it in obj.try_iter()? {
                let it = it?;
                if it.is_none() {
                    items.push(None);
                } else {
                    items.push(Some(build_variant(py, &it)?));
                }
            }
            *target = PvField::VariantArray(items);
            marks.set(bit);
        }
    }
    Ok(())
}
