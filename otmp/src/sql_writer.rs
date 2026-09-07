//! SQL mutation boundary shared by the candidate engine and `SQLite` replay oracle.
use std::num::NonZeroUsize;
use std::sync::Arc;

use rusqlite::{
    Connection, ToSql,
    types::{FromSql, ToSqlOutput, Value, ValueRef},
};

use crate::RuntimeError;

pub(crate) enum Writer<'a> {
    Sqlite(&'a Connection),
    Turso(&'a Arc<turso_core::Connection>),
}

pub(crate) struct Row(Vec<Value>);

impl Row {
    pub(crate) fn get<T: FromSql>(&self, index: usize) -> rusqlite::Result<T> {
        let value = self
            .0
            .get(index)
            .ok_or(rusqlite::Error::InvalidColumnIndex(index))?;
        T::column_result(ValueRef::from(value)).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(index, value.data_type(), Box::new(e))
        })
    }
}

impl Writer<'_> {
    pub(crate) fn ancestry(
        &self,
        mut tip: Option<otmp_protocol::Id>,
    ) -> Result<Vec<otmp_protocol::Id>, RuntimeError> {
        let mut chain = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        let mut last_sequence = u64::MAX;
        while let Some(id) = tip {
            if !seen.insert(id) {
                return Err(RuntimeError::Corrupt("snapshot ancestry cycle".into()));
            }
            let (parent, sequence): (Option<Vec<u8>>, i64) = self.query_row("SELECT parent_snapshot_id,sequence_number FROM otmp_snapshots WHERE snapshot_id=?1", rusqlite::params![id.as_bytes().as_slice()], |r| Ok((r.get(0)?, r.get(1)?)))?;
            if sequence <= 0 || sequence.cast_unsigned() >= last_sequence {
                return Err(RuntimeError::Corrupt(
                    "nondecreasing snapshot ancestry".into(),
                ));
            }
            last_sequence = sequence.cast_unsigned();
            chain.push(id);
            tip = parent
                .map(|bytes| {
                    otmp_protocol::Id::try_from_bytes(
                        bytes
                            .try_into()
                            .map_err(|_| RuntimeError::Corrupt("invalid snapshot ID".into()))?,
                    )
                    .map_err(RuntimeError::from)
                })
                .transpose()?;
        }
        Ok(chain)
    }
    fn prepare_turso(
        connection: &Arc<turso_core::Connection>,
        sql: &str,
        parameters: &[&dyn ToSql],
    ) -> Result<turso_core::Statement, RuntimeError> {
        let mut statement = connection.prepare(sql)?;
        for (index, parameter) in parameters.iter().enumerate() {
            let owned;
            let output = parameter.to_sql()?;
            let value = match output {
                ToSqlOutput::Borrowed(value) => value,
                ToSqlOutput::Owned(value) => {
                    owned = value;
                    ValueRef::from(&owned)
                }
                _ => return Err(RuntimeError::Turso("unsupported SQL parameter".into())),
            };
            let value = match value {
                ValueRef::Null => turso_core::Value::Null,
                ValueRef::Integer(value) => {
                    turso_core::Value::Numeric(turso_core::Numeric::Integer(value))
                }
                ValueRef::Real(_) => {
                    return Err(RuntimeError::Turso(
                        "floating SQL parameters are outside the metadata writer contract".into(),
                    ));
                }
                ValueRef::Text(value) => turso_core::Value::build_text(
                    std::str::from_utf8(value)
                        .map_err(|e| RuntimeError::Turso(e.to_string()))?
                        .to_owned(),
                ),
                ValueRef::Blob(value) => turso_core::Value::Blob(value.to_vec()),
            };
            statement.bind_at(NonZeroUsize::new(index + 1).unwrap(), value)?;
        }
        Ok(statement)
    }

    pub(crate) fn execute(
        &self,
        sql: &str,
        parameters: &[&dyn ToSql],
    ) -> Result<usize, RuntimeError> {
        match self {
            Self::Sqlite(connection) => Ok(connection.execute(sql, parameters)?),
            Self::Turso(connection) => {
                Self::prepare_turso(connection, sql, parameters)?.run_ignore_rows()?;
                usize::try_from(connection.changes())
                    .map_err(|_| RuntimeError::Turso("invalid change count".into()))
            }
        }
    }

    pub(crate) fn query_row<T>(
        &self,
        sql: &str,
        parameters: &[&dyn ToSql],
        read: impl FnOnce(&Row) -> rusqlite::Result<T>,
    ) -> Result<T, RuntimeError> {
        let values = match self {
            Self::Sqlite(connection) => connection.query_row(sql, parameters, |row| {
                (0..row.as_ref().column_count())
                    .map(|i| row.get(i))
                    .collect::<rusqlite::Result<Vec<Value>>>()
            })?,
            Self::Turso(connection) => {
                let mut rows =
                    Self::prepare_turso(connection, sql, parameters)?.run_collect_rows()?;
                if rows.is_empty() {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                rows.remove(0)
                    .into_iter()
                    .map(|value| match value {
                        turso_core::Value::Null => Ok(Value::Null),
                        turso_core::Value::Numeric(turso_core::Numeric::Integer(value)) => {
                            Ok(Value::Integer(value))
                        }
                        turso_core::Value::Numeric(turso_core::Numeric::Float(_)) => Err(
                            RuntimeError::Turso("unexpected float in metadata query".into()),
                        ),
                        turso_core::Value::Text(value) => Ok(Value::Text(value.to_string())),
                        turso_core::Value::Blob(value) => Ok(Value::Blob(value)),
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
        };
        Ok(read(&Row(values))?)
    }
}
