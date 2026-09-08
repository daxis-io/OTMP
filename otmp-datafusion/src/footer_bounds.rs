//! Allocation-free validation of compact-Thrift container lengths before native
//! Parquet decoding. Native decoders may reserve a declared list capacity before
//! discovering that its elements are missing from the footer.
use datafusion::parquet::errors::{ParquetError, Result};

fn invalid() -> ParquetError {
    ParquetError::General("invalid or excessive Parquet footer container bounds".into())
}

pub(crate) fn validate(bytes: &[u8]) -> Result<()> {
    let mut input = Input { bytes, at: 0 };
    input.value(12, true, 0)?;
    if input.at != bytes.len() {
        return Err(invalid());
    }
    Ok(())
}

struct Input<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl Input<'_> {
    fn byte(&mut self) -> Result<u8> {
        let value = *self.bytes.get(self.at).ok_or_else(invalid)?;
        self.at += 1;
        Ok(value)
    }
    fn skip(&mut self, count: usize) -> Result<()> {
        self.at = self
            .at
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(invalid)?;
        Ok(())
    }
    fn integer(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err(invalid());
            }
            value |= u64::from(byte & 127) << shift;
            if byte < 128 {
                return Ok(value);
            }
        }
        Err(invalid())
    }
    fn count(&self, count: u64) -> Result<usize> {
        let count = usize::try_from(count).map_err(|_| invalid())?;
        // Even an empty struct or binary value consumes at least one byte.
        if count > self.bytes.len() - self.at {
            return Err(invalid());
        }
        Ok(count)
    }
    fn value(&mut self, kind: u8, field: bool, depth: usize) -> Result<()> {
        if depth > 64 {
            return Err(invalid());
        }
        match kind {
            1 | 2 if field => (),
            1 | 2 => {
                if !matches!(self.byte()?, 1 | 2) {
                    return Err(invalid());
                }
            }
            3 => self.skip(1)?,
            4..=6 => {
                self.integer()?;
            }
            7 => self.skip(8)?,
            8 => {
                let length = self.integer()?;
                let length = self.count(length)?;
                self.skip(length)?;
            }
            9 | 10 => {
                let header = self.byte()?;
                let count = if header >> 4 == 15 {
                    self.integer()?
                } else {
                    u64::from(header >> 4)
                };
                let count = self.count(count)?;
                for _ in 0..count {
                    self.value(header & 15, false, depth + 1)?;
                }
            }
            11 => {
                let count = self.integer()?;
                if count != 0 {
                    let kinds = self.byte()?;
                    let count = self.count(count.checked_mul(2).ok_or_else(invalid)?)? / 2;
                    for _ in 0..count {
                        self.value(kinds >> 4, false, depth + 1)?;
                        self.value(kinds & 15, false, depth + 1)?;
                    }
                }
            }
            12 => loop {
                let header = self.byte()?;
                if header == 0 {
                    break;
                }
                if header >> 4 == 0 {
                    self.integer()?;
                }
                self.value(header & 15, true, depth + 1)?;
            },
            13 => self.skip(16)?,
            _ => return Err(invalid()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refuses_huge_declared_lists_before_native_capacity_allocation() {
        // field 1: list<struct>, declared i32::MAX elements with no payload.
        assert!(validate(&[0x19, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07, 0]).is_err());
        assert!(validate(&[0x18, 100, 0]).is_err());
        assert!(
            validate(&[
                0x15, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02, 0
            ])
            .is_err()
        );
        assert!(validate(&[0x19, 0x2c, 0, 0, 0]).is_ok());
    }
    #[test]
    fn limits_depth_and_accepts_inline_booleans_and_empty_collections() {
        assert!(validate(&[0x11, 0x19, 0, 0]).is_ok());
        let mut nested = vec![0x1c; 66];
        nested.extend([0; 67]);
        assert!(validate(&nested).is_err());
    }
}
