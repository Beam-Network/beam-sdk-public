//! Multipart part-number layout shared with BeamCore.
//!
//! `transfer-client-control/v7` uses consecutive part numbers: source chunk `i` is part `i + 1`
//! and the single attempt slot is always 0. Retries are recovered by BeamCore through staged
//! objects copied into the original part (see `provider_signing::sign_multipart_recovery`).
//! S3 caps part numbers at [`MULTIPART_MAX_PART_NUMBER`].

use crate::error::BeamApiError;

/// Highest part number S3-compatible multipart uploads accept.
pub const MULTIPART_MAX_PART_NUMBER: u64 = 10_000;
/// Part numbers reserved per source chunk.
pub const MULTIPART_ATTEMPT_SLOT_COUNT: u64 = 1;
/// Most source chunks a multipart destination can hold.
pub const MULTIPART_MAX_SOURCE_CHUNKS: u64 =
    MULTIPART_MAX_PART_NUMBER / MULTIPART_ATTEMPT_SLOT_COUNT;

/// The S3 part number for `chunk_index` (source-local); `attempt_slot` must be 0.
pub fn multipart_part_number(chunk_index: u64, attempt_slot: u64) -> Result<u64, BeamApiError> {
    if chunk_index >= MULTIPART_MAX_SOURCE_CHUNKS {
        return Err(BeamApiError::InvalidArgument(format!(
            "source_chunk_index must be less than {MULTIPART_MAX_SOURCE_CHUNKS}"
        )));
    }
    if attempt_slot >= MULTIPART_ATTEMPT_SLOT_COUNT {
        return Err(BeamApiError::InvalidArgument(
            "attempt_slot must be 0 for consecutive multipart uploads".to_string(),
        ));
    }
    Ok(chunk_index + 1)
}

/// The highest part number a multipart group with `chunk_count` source chunks can use.
pub(crate) fn multipart_max_part_number(chunk_count: u64) -> Result<u64, BeamApiError> {
    if chunk_count == 0 {
        return Err(BeamApiError::InvalidArgument(
            "source chunk_count must be a positive integer".to_string(),
        ));
    }
    multipart_part_number(chunk_count - 1, 0)
}

/// `part-number-marker` values for each 1,000-part ListParts page up to `max_part_number`.
pub(crate) fn multipart_list_page_markers(max_part_number: u64) -> Vec<u64> {
    (0..max_part_number).step_by(1_000).collect()
}

/// Index of the ListParts page that lists `part_number`.
pub(crate) fn multipart_list_page_index(part_number: u64) -> usize {
    (part_number.saturating_sub(1) / 1_000) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipart_parts_are_consecutive_with_a_10000_part_limit() {
        assert_eq!(multipart_part_number(0, 0).unwrap(), 1);
        assert_eq!(multipart_part_number(1, 0).unwrap(), 2);
        assert_eq!(multipart_part_number(9_999, 0).unwrap(), 10_000);
        assert!(multipart_part_number(10_000, 0)
            .unwrap_err()
            .to_string()
            .contains("less than 10000"));
        assert!(multipart_part_number(0, 1)
            .unwrap_err()
            .to_string()
            .contains("attempt_slot"));
        assert_eq!(MULTIPART_ATTEMPT_SLOT_COUNT, 1);
        assert_eq!(MULTIPART_MAX_SOURCE_CHUNKS, 10_000);
    }

    #[test]
    fn list_pages_cover_every_part() {
        assert_eq!(multipart_max_part_number(1).unwrap(), 1);
        assert_eq!(multipart_max_part_number(1_001).unwrap(), 1_001);
        assert_eq!(multipart_max_part_number(10_000).unwrap(), 10_000);
        assert!(multipart_max_part_number(10_001).is_err());
        assert_eq!(multipart_list_page_markers(1), vec![0]);
        assert_eq!(multipart_list_page_markers(1_001), vec![0, 1_000]);
        assert_eq!(multipart_list_page_index(1_000), 0);
        assert_eq!(multipart_list_page_index(1_001), 1);
        assert!(multipart_max_part_number(0).is_err());
    }
}
