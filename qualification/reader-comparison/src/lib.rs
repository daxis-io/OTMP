#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Expected {
    pub first: i64,
    pub count: i64,
    pub sum: i64,
}

pub fn expected_values(
    files: usize,
    rows_per_file: usize,
    survivors: Option<usize>,
) -> Result<Expected, &'static str> {
    if files == 0 || rows_per_file == 0 {
        return Err("files and rows_per_file must be positive");
    }
    let survivors = survivors.unwrap_or(files);
    if survivors > files {
        return Err("survivors exceeds file count");
    }
    let total_rows = files
        .checked_mul(rows_per_file)
        .ok_or("fixture row count overflows usize")?;
    let selected_rows = survivors
        .checked_mul(rows_per_file)
        .ok_or("selected row count overflows usize")?;
    let first = total_rows
        .checked_sub(selected_rows)
        .ok_or("invalid selected row count")?;
    let last = total_rows.checked_sub(1).ok_or("empty fixture")?;
    let count = i64::try_from(selected_rows).map_err(|_| "selected row count exceeds i64")?;
    let first = i64::try_from(first).map_err(|_| "first value exceeds i64")?;
    let last = i64::try_from(last).map_err(|_| "last value exceeds i64")?;
    let sum = i128::from(first)
        .checked_add(i128::from(last))
        .and_then(|ends| ends.checked_mul(i128::from(count)))
        .map(|twice_sum| twice_sum / 2)
        .and_then(|sum| i64::try_from(sum).ok())
        .ok_or("expected sum exceeds i64")?;
    Ok(Expected { first, count, sum })
}

#[cfg(test)]
mod tests {
    use super::{Expected, expected_values};

    #[test]
    fn selective_expectation_matches_last_two_fixture_files() {
        assert_eq!(
            expected_values(4_096, 128, Some(2)).unwrap(),
            Expected {
                first: 524_032,
                count: 256,
                sum: 134_184_832,
            }
        );
        assert_eq!(
            expected_values(16_384, 128, Some(2)).unwrap(),
            Expected {
                first: 2_096_896,
                count: 256,
                sum: 536_838_016,
            }
        );
    }

    #[test]
    fn all_files_expectation_uses_zero_threshold() {
        assert_eq!(
            expected_values(2, 3, None).unwrap(),
            Expected {
                first: 0,
                count: 6,
                sum: 15,
            }
        );
    }

    #[test]
    fn expectation_rejects_invalid_and_overflowing_inputs() {
        assert!(expected_values(0, 128, Some(2)).is_err());
        assert!(expected_values(4, 0, Some(2)).is_err());
        assert!(expected_values(4, 128, Some(5)).is_err());
        assert!(expected_values(usize::MAX, usize::MAX, None).is_err());
    }
}
