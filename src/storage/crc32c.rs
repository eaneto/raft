//! A small software CRC-32C (Castagnoli) implementation.
//!
//! `AGENTS.md` §8 rule 3 wants every on-disk record covered by a CRC32C. The
//! polynomial is the reflected Castagnoli constant `0x82F6_3B78`; this is the
//! same checksum SSE 4.2's `crc32` instruction computes, so a future
//! hardware-accelerated version stays compatible. Performance is not a goal
//! here (`AGENTS.md` §1), so this is the straightforward bit-at-a-time form
//! with no lookup table.

/// Reflected CRC-32C polynomial.
const POLY: u32 = 0x82f6_3b78;

/// The CRC-32C of `bytes`, with the standard `0xFFFF_FFFF` pre- and
/// post-conditioning.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            // Branchless: `mask` is all-ones when the low bit is set.
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLY & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn known_vectors() {
        // The two universally published CRC-32C check values.
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn a_flipped_bit_changes_the_checksum() {
        let mut data = *b"the quick brown fox";
        let good = crc32c(&data);
        data[7] ^= 0x01;
        assert_ne!(crc32c(&data), good);
    }

    #[test]
    fn appending_zero_bytes_still_changes_it() {
        // CRC-32C is not length-invariant: trailing zeros must matter, or a
        // truncated record could pass another record's checksum.
        assert_ne!(crc32c(b"abc"), crc32c(b"abc\0"));
    }
}
