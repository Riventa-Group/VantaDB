use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

/// Schema definition for a collection, enforced on insert and update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSchema {
    pub fields: Vec<FieldDef>,
    #[serde(default)]
    pub strict: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Number,
    Bool,
    Object,
    Array,
    Any,
}

impl CollectionSchema {
    pub fn validate(&self, doc: &Value) -> Result<(), Vec<String>> {
        let obj = match doc.as_object() {
            Some(o) => o,
            None => return Err(vec!["Document must be a JSON object".into()]),
        };

        let mut errors = Vec::new();

        for field_def in &self.fields {
            if field_def.name == "_id" {
                continue;
            }
            match obj.get(&field_def.name) {
                Some(val) => {
                    if let Err(mut errs) = validate_field(field_def, val) {
                        errors.append(&mut errs);
                    }
                }
                None => {
                    if field_def.required {
                        errors.push(format!("Missing required field '{}'", field_def.name));
                    }
                }
            }
        }

        if self.strict {
            let known: HashSet<&str> =
                self.fields.iter().map(|f| f.name.as_str()).collect();
            for key in obj.keys() {
                if key == "_id" {
                    continue;
                }
                if !known.contains(key.as_str()) {
                    errors.push(format!("Unknown field '{}' (strict mode)", key));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn from_json(val: &Value) -> Result<Self, String> {
        serde_json::from_value(val.clone()).map_err(|e| format!("Invalid schema: {}", e))
    }

    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

fn validate_field(def: &FieldDef, val: &Value) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    if def.field_type != FieldType::Any {
        let type_ok = match def.field_type {
            FieldType::String => val.is_string(),
            FieldType::Number => val.is_number(),
            FieldType::Bool => val.is_boolean(),
            FieldType::Object => val.is_object(),
            FieldType::Array => val.is_array(),
            FieldType::Any => true,
        };
        if !type_ok {
            errors.push(format!(
                "Field '{}': expected {}, got {}",
                def.name,
                format!("{:?}", def.field_type).to_lowercase(),
                value_type_name(val)
            ));
            return Err(errors);
        }
    }

    if let Some(n) = val.as_f64() {
        if let Some(min) = def.min {
            if n < min {
                errors.push(format!("Field '{}': {} < min {}", def.name, n, min));
            }
        }
        if let Some(max) = def.max {
            if n > max {
                errors.push(format!("Field '{}': {} > max {}", def.name, n, max));
            }
        }
    }

    if let Some(s) = val.as_str() {
        if let Some(min_len) = def.min_length {
            if s.len() < min_len {
                errors.push(format!(
                    "Field '{}': length {} < min {}",
                    def.name,
                    s.len(),
                    min_len
                ));
            }
        }
        if let Some(max_len) = def.max_length {
            if s.len() > max_len {
                errors.push(format!(
                    "Field '{}': length {} > max {}",
                    def.name,
                    s.len(),
                    max_len
                ));
            }
        }
        if let Some(ref pattern) = def.pattern {
            if let Ok(re) = regex::Regex::new(pattern) {
                if !re.is_match(s) {
                    errors.push(format!(
                        "Field '{}': doesn't match pattern '{}'",
                        def.name, pattern
                    ));
                }
            }
        }
    }

    if let Some(ref allowed) = def.enum_values {
        if !allowed.contains(val) {
            errors.push(format!("Field '{}': value not in allowed set", def.name));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
